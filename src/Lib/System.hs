module Lib.System () where

import           Data.Store (Store, encode) 
import           Control.Concurrent.Async (Async, async)
import           Control.Concurrent.STM.TMVar (TMVar, newEmptyTMVarIO, takeTMVar, putTMVar)
import           Control.Concurrent.STM.TVar (TVar, newTVarIO, readTVar, writeTVar)
import           Control.DeepSeq (force)
import           Control.Exception (evaluate)
import           Control.Concurrent.STM.TBMChan (TBMChan, writeTBMChan, readTBMChan)
import           Control.Concurrent.STM.TBChan (TBChan, writeTBChan, readTBChan)
import           Data.ByteString (ByteString)
import           Control.Concurrent.STM (STM, atomically)
import           Data.Map.Strict as Map (Map, insert, keys, empty, alter, insertLookupWithKey, updateLookupWithKey)
import qualified Data.ByteString as ByteString
import           Data.ByteString (ByteString)
import           System.IO (Handle, hFlush)
import           Data.Word (Word64)
import           Lib.StoreStream (sized)
import qualified Data.Vector.Storable as VS
import qualified Data.Vector as V
import           Data.LruCache as Lru (LruCache, empty, insert, lookup)
import           Data.Hashable (Hashable)
import           GHC.Generics (Generic)

newtype Segment = Segment Int
    deriving newtype (Eq,Ord,Hashable)

newtype Ix = Ix Int
    deriving newtype (Eq,Ord,Hashable)

newtype Offset = Offset Word64 
    deriving newtype (Eq, Ord, Store, VS.Storable)

-- Add constructor for sparse offsets
newtype StoredOffsets = OffsetVector (VS.Vector Offset)
    deriving newtype Store

plus :: Offset -> Int -> Offset
plus (Offset w) i = Offset (w + fromIntegral i)

data Ref = Ref !Segment !Ix
    deriving stock (Eq, Ord, Generic)
    deriving anyclass Hashable

newtype Flushed = Flushed (TMVar ())
mark :: Flushed -> IO ()
mark (Flushed mvar) = atomically $ putTMVar mvar ()

data WEnqueued = Write !(TMVar Ref) !Flushed !ByteString
               | Tick

data WriteQueue = WriteQueue (TBMChan WEnqueued)

data REnqueued = Read !Ref !(TMVar ByteString)
               | ReadComplete !Ref !ByteString

data ReadQueue = ReadQueue (TBChan REnqueued)

write :: Store a => WriteQueue -> a -> IO (Async (Ref, Flushed))
write (WriteQueue q) value = async $ do
    bs <- evaluate $ force $ encode $ sized value
    saved <- newEmptyTMVarIO
    flushed <- Flushed <$> newEmptyTMVarIO
    atomically $ writeTBMChan q $ Write saved flushed bs
    ref <- atomically $ takeTMVar saved
    return (ref, flushed)


data SegCache = SegCache StoredOffsets (LruCache Ix ByteString)

data ReaderConfig = ReaderConfig

data ReaderState = ReaderState {
    cached :: LruCache Ref ByteString,
    pending :: Map Ref [TMVar ByteString]
}


readerStep :: ReaderConfig -> ReaderState -> REnqueued -> IO (ReaderState, Maybe Ref)
readerStep ReaderConfig state@ReaderState{..} = \case
    Read ref done -> case Lru.lookup ref cached of
        Nothing -> 
            let combine _k new old = new ++ old in
            case Map.insertLookupWithKey combine ref [done] pending of
                (Nothing, pending') -> return (state {pending = pending'}, Just ref)
                (Just _waiting, pending') -> return (state {pending = pending'}, Nothing)
        Just (string, cached') -> do
            atomically (putTMVar done string)
            return (state {cached = cached'}, Nothing)
    ReadComplete ref string -> 
        let remove _k _v = Nothing in
        case Map.updateLookupWithKey remove ref pending of
            (Nothing, _) -> error "Pending read completed, but no one was waiting for it"
            (Just waiting, pending') -> do
                mapM_ (\done -> atomically (putTMVar done string)) waiting
                return (state {pending = pending'}, Nothing)


-- Sharded by segment hash
data Readers = Readers {shardShift :: Int, 
                        shards :: V.Vector ReadQueue}

data ReadCache = ReadCache {
        readers :: Readers,
        hotSegment :: Segment,
        hot :: TVar (Map Offset ByteString)
    }

data ConsumerLimits = ConsumerLimits {
        cutoffCount :: Int,
        cutoffLength :: Offset
    }



data FinishedWith = Cutover | QueueDepleted

consume :: ConsumerLimits -> ConsumerConfig -> WriteQueue -> IO FinishedWith
consume ConsumerLimits{..} cfg@ConsumerConfig{..} (WriteQueue q) = go initial
    where
    initial = ConsumerState Map.empty (Offset 0) []
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

consume' :: ConsumerConfig -> ConsumerState -> WEnqueued -> IO ConsumerState
consume' ConsumerConfig{..} state@ConsumerState{..} = \case
    Tick -> if null flushQueue
        then return state 
        else do
            hFlush commandLog
            mapM_ mark flushQueue
            return (state {flushQueue = []})
    Write refVar flushed bs -> do
        let entries' = Map.insert offset bs entries
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
    let offsets = OffsetVector (VS.fromList (keys entries))
        storedOffsets = encode offsets
        offsetsLen = fromIntegral (ByteString.length storedOffsets) :: Word64 
    ByteString.hPut commandLog storedOffsets
    ByteString.hPut commandLog $ encode offsetsLen


