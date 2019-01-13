module Lib.System () where

import           Data.Store (Store, encode, decodeEx) 
import           Control.Concurrent.Async (Async, async)
import           Control.Concurrent (forkIO)
import           Control.Concurrent.STM.TMVar (TMVar, newEmptyTMVarIO, takeTMVar, putTMVar)
import           Control.Concurrent.STM.TVar (TVar, newTVarIO, readTVar, writeTVar)
import           Control.DeepSeq (NFData, force)
import           Control.Exception (evaluate)
import           Control.Concurrent.STM.TBMChan (TBMChan, writeTBMChan, readTBMChan)
import           Control.Concurrent.STM.TBChan (TBChan, writeTBChan, readTBChan)
import           Data.ByteString (ByteString)
import           Control.Concurrent.STM (STM, atomically)
import           Data.Map.Strict as Map (Map, insert, keys, empty, alter, insertLookupWithKey, updateLookupWithKey, adjust, delete, lookup, singleton, traverseWithKey)
import qualified Data.ByteString as ByteString
import           Data.ByteString (ByteString)
import           System.IO (Handle, hFlush)
import           Data.Word (Word64)
import           Lib.StoreStream (sized)
import qualified Data.Vector.Storable as VS
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Generic as VG
import qualified Data.Vector as V
import           Data.LruCache as Lru (LruCache, empty, insert, lookup, insertView)
import           Data.Hashable (Hashable)
import           GHC.Generics (Generic)
import           Foreign.Ptr (Ptr, plusPtr, castPtr)
import           Foreign.Storable (Storable, peek, poke, sizeOf, alignment)

newtype Segment = Segment Word64
    deriving newtype (Eq,Ord,Hashable, Store, Storable)

newtype Ix = Ix Word64
    deriving newtype (Eq,Ord,Hashable, Store, Storable)

newtype Offset = Offset Word64 
    deriving newtype (Eq, Ord, Store, Storable)

data Assoc = Assoc !Ix !Offset

assocPtrs :: Ptr Assoc -> (Ptr Ix, Ptr Offset)
assocPtrs ptr = (castPtr ptr, castPtr ptr `plusPtr` 8)
instance Storable Assoc where
    sizeOf _ = 16
    alignment _ = 8
    peek ap = let (ip,op) = assocPtrs ap in Assoc <$> peek ip <*> peek op
    poke ap (Assoc i o) = let (ip,op) = assocPtrs ap in poke ip i >> poke op o


-- Add constructor for sparse offsets
data StoredOffsets = OffsetVector (VS.Vector Offset)
                   | SparseVector (VS.Vector Assoc)
    deriving stock Generic
    deriving anyclass (Store, NFData)

offset :: StoredOffsets -> Ix -> Offset
offset (OffsetVector v) (Ix i) = v VS.! (fromIntegral i)


plus :: Offset -> Int -> Offset
plus (Offset w) i = Offset (w + fromIntegral i)

data Ref = Ref !Segment !Ix
    deriving stock (Eq, Ord, Generic)
    deriving anyclass (Hashable, Store)
refPtrs :: Ptr Ref -> (Ptr Segment, Ptr Ix)
refPtrs ptr = (castPtr ptr, castPtr ptr `plusPtr` 8)
instance Storable Ref where
    sizeOf _ = 16
    alignment _ = 8
    peek rp = let (sp,ip) = refPtrs rp in Ref <$> peek sp <*> peek ip
    poke rp (Ref s i) = let (sp,ip) = refPtrs rp in poke sp s >> poke ip i

newtype Flushed = Flushed (TMVar ())
mark :: Flushed -> IO ()
mark (Flushed mvar) = atomically $ putTMVar mvar ()

data WEnqueued = Write !(TMVar Ref) !Flushed !ByteString
               | Tick

data WriteQueue = WriteQueue (TBMChan WEnqueued)

data REnqueued = Read !Ref !(TMVar ByteString)
               | ReadComplete !StoredOffsets !Segment !ByteString
               | SegmentWasGCed !StoredOffsets !Segment !ByteString -- The new segment (mmapped)

data ReadQueue = ReadQueue (TBChan REnqueued)

write :: Store a => WriteQueue -> a -> IO (Async (Ref, Flushed))
write (WriteQueue q) value = async $ do
    bs <- evaluate $ force $ encode $ sized value
    saved <- newEmptyTMVarIO
    flushed <- Flushed <$> newEmptyTMVarIO
    atomically $ writeTBMChan q $ Write saved flushed bs
    ref <- atomically $ takeTMVar saved
    return (ref, flushed)


-- data SegCache = SegCache StoredOffsets (LruCache Ix ByteString)

data ReaderConfig = ReaderConfig {
        openSeg     :: Segment -> IO ByteString,
        lruSize     :: Int,
        maxOpenSegs :: Int
    }

-- Holds all the open segment files (per shard)
data SegmentCache = SegmentCache {
    openSegments :: LruCache Segment (), 
    openSegmentDetails :: Map Segment (StoredOffsets, ByteString) -- The ByteString is an mmapped view of the file
}

emptySegmentCache :: Int -> SegmentCache 
emptySegmentCache max = SegmentCache {openSegments = Lru.empty max, openSegmentDetails = Map.empty}


loadOffsets :: ByteString -> StoredOffsets
loadOffsets bs | ByteString.length bs < 8 = error "Segment is too short to contain offset cache length tag"
               | otherwise = 
                    let last n = ByteString.drop (ByteString.length bs - n) bs in 
                    let offsetLen = fromIntegral (decodeEx (last 8) :: Word64) in
                    if | ByteString.length bs < 8 + offsetLen -> error "Segment is too short to contain purported offset cache"
                       | otherwise -> let offsetsBs = ByteString.take offsetLen (last (offsetLen + 8)) in 
                                      decodeEx offsetsBs :: StoredOffsets

updateSegment :: Segment -> StoredOffsets -> ByteString -> SegmentCache -> SegmentCache
updateSegment segment offsets new cache@SegmentCache{..} = 
    cache {openSegmentDetails = Map.adjust (const (offsets, new)) segment openSegmentDetails}

insertSegment :: Segment -> StoredOffsets -> ByteString -> SegmentCache -> SegmentCache
insertSegment segment offsets bs cache@SegmentCache{..} =  SegmentCache open' details'
    where
    (removed,open') = Lru.insertView segment () openSegments 
    details' = case removed of
        Nothing -> Map.insert segment (offsets,bs) openSegmentDetails 
        Just (removed,()) -> Map.insert segment (offsets, bs) $ Map.delete removed openSegmentDetails 

readSeg :: StoredOffsets -> ByteString -> Ix -> ByteString
readSeg offs bs ix = value
    where
    Offset theOffset = offset offs ix
    len :: Word64 = decodeEx $ ByteString.take 8 $ ByteString.drop (fromIntegral theOffset) bs
    value = ByteString.take (fromIntegral len) $ ByteString.drop (fromIntegral theOffset + 8) bs

readSegCache :: SegmentCache -> Ref -> Maybe (ByteString, SegmentCache)
readSegCache cache@SegmentCache{..} (Ref seg ix) = 
    case Map.lookup seg openSegmentDetails of
        Nothing -> Nothing
        Just (offs,bs) -> Just (value, cache {openSegments = open'})
            where
            value = readSeg offs bs ix
            open' = case Lru.lookup seg openSegments of
                Just ((), new) -> new
                Nothing -> error "A cache segment is present in the details map, but not the open segment LRU"


data ReaderState = ReaderState {
    cached :: LruCache Ref ByteString,
    segmentCache :: SegmentCache,
    pending :: Map Segment (Map Ix [TMVar ByteString])
}

data Found a = NoK1 | NoK2 | Found a

-- There's bound to be a good single-pass way of doing this, but it will take a bit to figure out
appsert :: (Ord k1, Ord k2) => (Maybe a -> a) -> k1 -> k2  -> Map k1 (Map k2 a) -> (Map k1 (Map k2 a), Found a)
appsert f k1 k2 m1 = (m1', found)
    where
    found = case Map.lookup k1 m1 of
        Nothing -> NoK1
        Just m2 -> case Map.lookup k2 m2 of
            Nothing -> NoK2
            Just a -> Found a
    a' = case found of 
        Found a -> f (Just a)
        _ -> f Nothing
    m1' = Map.alter g k1 m1
    g Nothing = Just (Map.singleton k2 a')
    g (Just m2) = Just (Map.insert k2 a' m2)




reader :: ReaderConfig -> ReadQueue -> IO void
reader ReaderConfig{..} (ReadQueue q) = go initial
    where
    initial = ReaderState (Lru.empty lruSize) (emptySegmentCache maxOpenSegs) Map.empty
    go state = do
        enqueued <- atomically $ readTBChan q
        (state', toDispatch) <- readerStep state enqueued
        case toDispatch of
            Nothing -> return ()
            Just seg -> do
                forkIO $ do
                    bs <- openSeg seg
                    offsets <- evaluate $ force $ loadOffsets bs
                    atomically $ writeTBChan q (ReadComplete offsets seg bs)
                return ()
        go state'


readerStep :: ReaderState -> REnqueued -> IO (ReaderState, Maybe Segment)
readerStep state@ReaderState{..} = \case
    Read ref@(Ref seg ix) done -> case Lru.lookup ref cached of
        Just (string, cached') -> do
            atomically (putTMVar done string)
            return (state {cached = cached'}, Nothing)
        Nothing -> case readSegCache segmentCache ref of
            Just (string, cache') -> do
                atomically (putTMVar done string)
                return (state {segmentCache = cache'}, Nothing)
            Nothing -> 
                let (pending', waiting) = appsert (maybe [done] (done:)) seg ix pending in
                case waiting of
                    NoK1    -> return (state {pending = pending'}, Just seg)
                    NoK2    -> return (state {pending = pending'}, Nothing)
                    Found _ -> return (state {pending = pending'}, Nothing)
    ReadComplete offsets seg string -> 
        let remove _k _v = Nothing in
        case Map.updateLookupWithKey remove seg pending of
            (Nothing, _) -> error "Pending read completed, but no one was waiting for it"
            (Just waiting, pending') -> do
                let cache' = insertSegment seg offsets string segmentCache
                    clear ix dones = 
                        let value = readSeg offsets string ix in 
                        mapM_ (\done -> atomically (putTMVar done value)) dones
                Map.traverseWithKey clear waiting
                return (state {pending = pending', segmentCache = cache'}, Nothing)
    SegmentWasGCed offsets seg new -> 
        let cache' = updateSegment seg offsets new segmentCache in 
        return (state {segmentCache = cache'}, Nothing)


-- Sharded by segment hash
data Readers = Readers {shardShift :: Int, 
                        shards :: V.Vector ReadQueue}

data ReadCache = ReadCache {
        readers :: Readers,
        -- Everyone has to read this for every read. Seems bad. But weak IORef MM causes issues
        hot :: TVar (Segment,Map Offset ByteString)
    }

data ConsumerLimits = ConsumerLimits {
        cutoffCount :: Int,
        cutoffLength :: Offset
    }

-- todo: Lift out some of the logic from the consumer to reuse here
-- gc :: (ByteString -> VS.Vector Ref) -> (Ref -> Bool) -> StoredOffsets -> ByteString -> (ByteString -> m ())

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
        if writeOffset state' >= cutoffLength || length (entries state) >= cutoffCount
            then finalize cfg state' >> return Cutover
            else go state'

data ConsumerConfig = ConsumerConfig {
        commandLog :: Handle,
        segment :: Segment,
        register :: Map Offset ByteString -> IO () -- Should save in "hot" of ReadCache
    }

data ConsumerState = ConsumerState {
        entries :: Map Offset ByteString,
        writeOffset :: Offset,
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
        let entries' = Map.insert writeOffset bs entries
            writeOffset' = writeOffset `plus` ByteString.length bs
            ix = Ix (fromIntegral $ length entries)
        ByteString.hPut commandLog bs
        -- This is a bit confusing: register is supposed to update the shared "hot" cache for the active write head.
        -- We *must* make a globally visible update to the hot cache before returning the ref, or else
        -- someone could try to do a read that would fail.
        -- I *think* if we use STM vars to keep track of the hot cache and also to return the ref,
        -- we should be fine - even if the updates happen in separate atomic blocks. But not 100% sure.
        register entries'
        atomically $ putTMVar refVar (Ref segment ix)
        return (state {entries = entries', writeOffset = writeOffset', flushQueue = flushed : flushQueue})



finalize :: ConsumerConfig -> ConsumerState -> IO ()
finalize ConsumerConfig{..} ConsumerState{..} = do
    hFlush commandLog
    mapM_ mark flushQueue
    let offsets = OffsetVector (VS.fromList (keys entries))
        storedOffsets = encode offsets
        offsetsLen = fromIntegral (ByteString.length storedOffsets) :: Word64 
    ByteString.hPut commandLog $ encode (maxBound :: Word64)
    ByteString.hPut commandLog storedOffsets
    ByteString.hPut commandLog $ encode offsetsLen


