module Lib.System () where

import           Data.Store (Store, encode) 
import           Control.Concurrent.Async (Async, async)
import           Control.Concurrent.STM.TMVar (TMVar, newEmptyTMVarIO, takeTMVar, putTMVar)
import           Control.Concurrent.STM.TVar (TVar, newTVarIO, readTVar, writeTVar)
import           Control.DeepSeq (force)
import           Control.Exception (evaluate)
import           Control.Concurrent.STM.TBMChan (TBMChan, writeTBMChan, readTBMChan)
import           Data.ByteString (ByteString)
import           Control.Concurrent.STM (STM, atomically)
import           Data.Map.Strict (Map, insert, keys, empty)
import qualified Data.ByteString as ByteString
import           Data.ByteString (ByteString)
import           System.IO (Handle, hFlush)
import           Data.Word (Word64)
import           Lib.StoreStream (sized)
import qualified Data.Vector.Storable as VS

newtype Segment = Segment Int

newtype Ix = Ix Int

newtype Offset = Offset Word64 
    deriving newtype (Eq, Ord, Store, VS.Storable)

plus :: Offset -> Int -> Offset
plus (Offset w) i = Offset (w + fromIntegral i)

data Ref = Ref !Segment !Ix

newtype Flushed = Flushed (TMVar ())
mark :: Flushed -> IO ()
mark (Flushed mvar) = atomically $ putTMVar mvar ()

data Enqueued = Write !(TMVar Ref) !Flushed !ByteString
              | Tick

data WriteQueue = WriteQueue (TBMChan Enqueued)

write :: Store a => WriteQueue -> a -> IO (Async (Ref, Flushed))
write (WriteQueue q) value = async $ do
    bs <- evaluate $ force $ encode $ sized value
    saved <- newEmptyTMVarIO
    flushed <- Flushed <$> newEmptyTMVarIO
    atomically $ writeTBMChan q $ Write saved flushed bs
    ref <- atomically $ takeTMVar saved
    return (ref, flushed)


data ConsumerLimits = ConsumerLimits {
        cutoffCount :: Int,
        cutoffLength :: Offset
    }

data FinishedWith = Cutover | QueueDepleted

consume :: ConsumerLimits -> ConsumerConfig -> WriteQueue -> IO FinishedWith
consume ConsumerLimits{..} cfg@ConsumerConfig{..} (WriteQueue q) = go initial
    where
    initial = ConsumerState empty (Offset 0) []
    go state = do
            enqueued <- atomically $ readTBMChan q
            case enqueued of
                Nothing -> return QueueDepleted
                Just item -> step state item
    step state enqueued = do
        state' <- consume' cfg state enqueued
        -- We put the finalizer check here instead of on the LHS of go
        -- so that we always make progress, even if settings are too restrictive
        if offset state' >= cutoffLength || length (entries state) >= cutoffCount
            then finalize cfg state' >> return Cutover
            else go state'

data ConsumerConfig = ConsumerConfig {
        commandLog :: Handle,
        segment :: Segment,
        sharedCache :: TVar (Map Offset ByteString) -- Cannot use IORef, on account of its weak memory model
    }

data ConsumerState = ConsumerState {
        entries :: Map Offset ByteString,
        offset :: Offset,
        flushQueue :: [Flushed]
    }

consume' :: ConsumerConfig -> ConsumerState -> Enqueued -> IO ConsumerState
consume' ConsumerConfig{..} state@ConsumerState{..} = \case
    Tick -> if null flushQueue
        then return state 
        else do
            hFlush commandLog
            mapM_ mark flushQueue
            return (state {flushQueue = []})
    Write refVar flushed bs -> do
        let entries' = insert offset bs entries
            offset' = offset `plus` ByteString.length bs
            ix = Ix (length entries)
        ByteString.hPut commandLog bs
        -- I'm pretty sure that putting these in separate "atomically" blocks still provides a strong MM w/o reordering
        -- If it doesn't, this is wrong
        atomically $ writeTVar sharedCache entries'
        atomically $ putTMVar refVar (Ref segment ix)
        return (state {entries = entries', offset = offset', flushQueue = flushed : flushQueue})

finalize :: ConsumerConfig -> ConsumerState -> IO ()
finalize ConsumerConfig{..} ConsumerState{..} = do
    hFlush commandLog
    mapM_ mark flushQueue
    let offsets = VS.fromList (keys entries)
        storedOffsets = encode offsets
        offsetsLen = fromIntegral (ByteString.length storedOffsets) :: Word64 
    ByteString.hPut commandLog storedOffsets
    ByteString.hPut commandLog $ encode offsetsLen


