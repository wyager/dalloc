module Lib.System  where

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
import           Data.Set as Set (Set, insert, member, empty)
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
import           Streaming.Prelude (Stream, Of, yield, breakWhen, foldM)
import           Data.Monoid (Sum(..))
import           Control.Monad.Trans.Class (lift)
import           Control.Monad.Morph (hoist)

newtype Segment = Segment Word64
    deriving newtype (Eq,Ord,Hashable, Store, Storable, Show)

newtype Ix = Ix Word64
    deriving newtype (Eq,Ord,Hashable, Store, Storable, Show)

newtype Offset = Offset Word64 
    deriving newtype (Eq, Ord, Store, Storable, Num, Show)

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

data Ref = Ref {refSegment :: !Segment, refIx :: !Ix}
    deriving stock (Eq, Ord, Generic, Show)
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

data WEnqueued flushed ref
    = Write !ref !flushed !ByteString
    | Tick

data MMapped = MMapped !StoredOffsets !ByteString

data WriteQueue = WriteQueue (TBMChan (WEnqueued Flushed (TMVar Ref)))

data REnqueued = Read !Ref !(TMVar ByteString)
               | ReadComplete !Segment !MMapped
               | SegmentWasGCed !Segment !MMapped -- The new segment (mmapped)

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
    openSegmentDetails :: Map Segment MMapped -- The ByteString is an mmapped view of the file
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

updateSegment :: Segment -> MMapped -> SegmentCache -> SegmentCache
updateSegment segment mmapped cache@SegmentCache{..} = 
    cache {openSegmentDetails = Map.adjust (const mmapped) segment openSegmentDetails}

insertSegment :: Segment -> MMapped -> SegmentCache -> SegmentCache
insertSegment segment mmapped cache@SegmentCache{..} =  SegmentCache open' details'
    where
    (removed,open') = Lru.insertView segment () openSegments 
    details' = case removed of
        Nothing -> Map.insert segment mmapped openSegmentDetails 
        Just (removed,()) -> Map.insert segment mmapped $ Map.delete removed openSegmentDetails 


data Order = Asc | Desc
streamSegFromOffsets :: Monad m => Order -> MMapped -> Stream (Of (Ix, ByteString)) m ()
streamSegFromOffsets order (MMapped stored bs) = mapM_ (\(ix,off) -> yield (ix, readOff bs off)) offsets
    where 
    offsets = case order of
        Asc -> inorder
        Desc -> reverse inorder -- Can optimize this whole thing to remove list, if needed
    inorder = case stored of
        OffsetVector v ->  zip (fmap Ix [0..]) (VS.toList v)
        SparseVector v -> fmap (\(Assoc ix off) -> (ix,off)) $ VS.toList v



readOff :: ByteString -> Offset ->  ByteString
readOff bs (Offset theOffset) = value
    where
    len :: Word64 = decodeEx $ ByteString.take 8 $ ByteString.drop (fromIntegral theOffset) bs
    value = ByteString.take (fromIntegral len) $ ByteString.drop (fromIntegral theOffset + 8) bs

readSeg :: MMapped-> Ix -> ByteString
readSeg (MMapped offs bs) ix = readOff bs (offset offs ix) 

    

readSegCache :: SegmentCache -> Ref -> Maybe (ByteString, SegmentCache)
readSegCache cache@SegmentCache{..} (Ref seg ix) = 
    case Map.lookup seg openSegmentDetails of
        Nothing -> Nothing
        Just mmapped -> Just (value, cache {openSegments = open'})
            where
            value = readSeg mmapped ix
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
appsert f k1 k2 map = (map', found)
    where
    find = maybe NoK1 (maybe NoK2 Found . Map.lookup k2) . Map.lookup k1
    found = find map
    a' = case found of 
        Found a -> f (Just a)
        _ -> f Nothing
    map' = Map.alter g k1 map
    g Nothing = Just (Map.singleton k2 a')
    g (Just inner) = Just (Map.insert k2 a' inner)




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
                    atomically $ writeTBChan q (ReadComplete seg $ MMapped offsets bs)
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
    ReadComplete  seg mmapped -> 
        let remove _k _v = Nothing in
        case Map.updateLookupWithKey remove seg pending of
            (Nothing, _) -> error "Pending read completed, but no one was waiting for it"
            (Just waiting, pending') -> do
                let cache' = insertSegment seg mmapped segmentCache
                    clear ix dones = 
                        let value = readSeg mmapped ix in 
                        mapM_ (\done -> atomically (putTMVar done value)) dones
                Map.traverseWithKey clear waiting
                return (state {pending = pending', segmentCache = cache'}, Nothing)
    SegmentWasGCed seg mmapped -> 
        let cache' = updateSegment seg mmapped segmentCache in 
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

exceeds :: (Int, Offset) -> ConsumerLimits -> Bool
(count,len) `exceeds` ConsumerLimits{..} = count >= cutoffCount || len >= cutoffLength

splitWith :: Monad m
          => ConsumerLimits 
          -> Stream (Of (WEnqueued flushed ref)) m a 
          -> Stream (Of (WEnqueued flushed ref)) m 
           ((Stream (Of (WEnqueued flushed ref)) m a))
splitWith limits = breakWhen throughput (0,0) id (`exceeds` limits) 
    where
    throughput (count,len) (Write _ _ bs) = (count + 1, len `plus` ByteString.length bs)


consume :: Monad m
        => (ConsumerAction flushed ref -> m ()) 
        -> (ConsumerState flushed -> m o)
        -> Stream (Of (WEnqueued flushed ref)) m r -> m (Of o r)
consume handle finalize = foldM step initial finalize
    where
    initial = return (ConsumerState Map.empty (Offset 0) [])
    step state write = do
        let (state', action) = consume' state write
        handle action
        return state'



data GcConfig entry = GcConfig {
    thisSegment :: Segment,
    childrenOf :: entry -> VS.Vector Ref
}

data GcState = GcState {
    liveHere :: Set Ix,
    liveThere :: Set Ref
}




gc :: Monad m => GcConfig entry -> Set Ix -> Stream (Of (Ix, entry)) m o -> Stream (Of (Ix, entry)) m (Of (Set Ref) o)
gc GcConfig{..} here = foldM step (return (GcState here Set.empty)) (return . liveThere) . hoist lift
    where
    step state (ix,bs) = if ix `Set.member` (liveHere state)
                            then do
                                yield (ix,bs)
                                return (step' state ix bs)
                            else return state
    step' state thisIx bs = VS.foldl' step state children
        where
        children = childrenOf bs
        step s@GcState{..} ref@(Ref seg ix)
            | seg > thisSegment = error "Segment causality violation"
            | seg == thisSegment && ix >= thisIx = error "Index causality violation"
            | seg == thisSegment = s {liveHere = Set.insert ix liveHere}
            | otherwise = s {liveThere = Set.insert ref liveThere}

repackFile :: Handle -> Stream (Of (Ix, ByteString)) IO r -> IO r
repackFile = undefined -- Save entries, then save the stored offsets

data ConsumerConfig = ConsumerConfig {
        commandLog :: Handle,
        segment :: Segment,
        register :: Map Offset ByteString -> IO () -- Should save in "hot" of ReadCache
    }

data ConsumerState flushed = ConsumerState {
        entries :: Map Offset ByteString,
        writeOffset :: Offset,
        flushQueue :: [flushed]
    }


data Step = Incomplete | Finalizer ByteString | Packet ByteString ByteString
step :: ByteString -> Step
step bs = if | totalLen < 8 -> Incomplete
             | packetLen == maxBound -> Finalizer afterLen
             | ByteString.length bs < 8 + packetLen' -> Incomplete
             | otherwise -> Packet packet remainder
        where
        totalLen = ByteString.length bs
        packetLen :: Word64 = decodeEx $ ByteString.take 8 bs
        packetLen' :: Int = fromIntegral packetLen
        afterLen = ByteString.drop 8 bs
        (packet,remainder) = ByteString.splitAt packetLen' afterLen

stream :: Monad m => ByteString -> Stream (Of ByteString) m (Maybe ByteString)
stream = go
    where
    go bs = case step bs of
        Incomplete -> return Nothing
        Finalizer next -> case step next of
            Packet bs _ -> return (Just bs)
            Incomplete -> return Nothing
            Finalizer _ -> error "Repeated finalizer"
        Packet packet next -> do
            yield packet
            go next


data ConsumerAction flushed ref = Flush [flushed] | WriteToLog ByteString (Map Offset ByteString) ref Ix | Nop



consume' :: ConsumerState flushed -> WEnqueued flushed ref -> (ConsumerState flushed, ConsumerAction flushed ref)
consume' state@ConsumerState{..} = \case
    Tick -> if null flushQueue
        then (state, Nop)
        else (state {flushQueue = []}, Flush flushQueue)
    Write refVar flushed bs -> do
        let entries' = Map.insert writeOffset bs entries
            writeOffset' = writeOffset `plus` ByteString.length bs
            ix = Ix (fromIntegral $ length entries)
        (state {entries = entries', writeOffset = writeOffset', flushQueue = flushed : flushQueue},
            WriteToLog bs entries' refVar ix)


consumeFile :: ConsumerConfig -> Stream (Of (WEnqueued Flushed (TMVar Ref))) IO r -> IO (Of () r)
consumeFile cfg = consume (saveFile cfg) (finalizeFile cfg)

saveFile :: ConsumerConfig -> ConsumerAction Flushed (TMVar Ref) -> IO ()
saveFile ConsumerConfig{..} = \case
        Flush flushed -> do
            hFlush commandLog
            mapM_ mark flushed
        WriteToLog bs entries refVar ix -> do
            ByteString.hPut commandLog bs
            -- This is a bit confusing: register is supposed to update the shared "hot" cache for the active write head.
            -- We *must* make a globally visible update to the hot cache before returning the ref, or else
            -- someone could try to do a read that would fail.
            -- I *think* if we use STM vars to keep track of the hot cache and also to return the ref,
            -- we should be fine - even if the updates happen in separate atomic blocks. But not 100% sure.
            register entries
            atomically $ putTMVar refVar (Ref segment ix) 

finalizeFile :: ConsumerConfig -> ConsumerState Flushed -> IO ()
finalizeFile ConsumerConfig{..} ConsumerState{..} = do
    hFlush commandLog
    mapM_ mark flushQueue
    let offsets = OffsetVector (VS.fromList (keys entries))
        storedOffsets = encode offsets
        offsetsLen = fromIntegral (ByteString.length storedOffsets) :: Word64 
    ByteString.hPut commandLog $ encode (maxBound :: Word64)
    ByteString.hPut commandLog $ encode offsetsLen
    ByteString.hPut commandLog storedOffsets -- length on either side so we can read from the end of file or in order
    ByteString.hPut commandLog $ encode offsetsLen
    hFlush commandLog


