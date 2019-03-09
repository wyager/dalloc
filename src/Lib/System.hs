{-# LANGUAGE UndecidableInstances #-} -- So that we can GND-derive MonadConc instances for transformers 
module Lib.System where

import           Data.Store (Store, encode, decode, decodeEx, PeekException) 
import           Control.Concurrent.Classy.Async (Async, async, link, waitAny, wait, waitCatch)
-- import           Control.Concurrent (forkIO)
import           Control.Concurrent.Classy.MVar (MVar, newEmptyMVar, takeMVar, putMVar, swapMVar, readMVar, newMVar)
-- import           Control.Concurrent.STM.TVar (TVar)
import           Control.DeepSeq (NFData, rnf, force)
import           Control.Exception (Exception,evaluate, throwIO, SomeException, catch)
import           Control.Concurrent.Classy.Chan (Chan, newChan, readChan, writeChan)
import           Data.Map.Strict as Map (Map, insert, keys, empty, alter, updateLookupWithKey, adjust, delete, lookup, singleton, traverseWithKey, elemAt)
import qualified Data.Map.Strict as Map (toList)
import           Data.Set as Set (Set, insert, member, empty, union, spanAntitone)
import qualified Data.Set as Set (map)
import qualified Data.ByteString as ByteString
import           Data.ByteString (ByteString)
import           Data.ByteString.Builder (Builder, byteString, hPutBuilder)
import           Data.Foldable (toList)
import           System.IO (Handle, hFlush, IOMode(WriteMode), openFile)
import           System.IO.MMap (mmapFileByteString)
import           System.Directory (doesFileExist, renameFile, removeFile)
import           Data.Word (Word64)
import           Lib.StoreStream (sized)
import qualified Data.Vector.Storable as VS
import qualified Data.Vector as V
import           Data.LruCache as Lru (LruCache, empty, lookup, insertView)
import           Data.Hashable (Hashable, hash)
import           GHC.Generics (Generic)
import           Foreign.Ptr (Ptr, plusPtr, castPtr)
import           Foreign.Storable (Storable, peek, poke, sizeOf, alignment)
import           Streaming.Prelude (Stream, yield, foldM, next)
import qualified Streaming.Prelude as SP (mapM)
import           Streaming (wrap, mapsM_)
import           Data.Functor.Of (Of((:>)))
import           Control.Monad.Trans.Class (lift)
import           Control.Monad.Morph (MonadTrans, hoist)
import           Numeric.Search.Range (searchFromTo)
import           Numeric.Search (searchM, divForever, hiVal)
-- import           Control.Monad.Error.Class (MonadError, throwError)
import           Control.Monad.IO.Class (MonadIO, liftIO)
import           Control.Monad.Except (ExceptT, throwError)
import           GHC.Exts (Constraint)
import           Type.Reflection (Typeable)
import           Data.Void (Void, absurd)
import           Control.Monad.Reader (ReaderT(..), ask)
import           Control.Monad.State.Strict (StateT(..), modify, get, put, evalStateT)
import           Control.Monad.Identity (Identity(..))
import           Control.Monad.Random.Class (MonadRandom, getRandomR)
import           Control.Monad (replicateM, forever)
import           Data.Proxy (Proxy(Proxy))
import           Data.Bits ((.&.), shiftL)
import           Control.Concurrent.Classy (MonadConc)
import           Control.Monad.Catch (MonadThrow, MonadCatch, MonadMask)

newtype Segment = Segment Word64
    deriving newtype (Eq,Ord,Hashable, Store, Storable, Show, Enum, Bounded)

newtype Ix = Ix Word64
    deriving newtype (Eq,Ord,Hashable, Store, Storable, Show, Enum, Bounded)

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


data IndexError = IndexNotFound Ix deriving Show

offset :: Throws '[IndexError] m =>  StoredOffsets -> Ix -> m Offset
offset (OffsetVector v) (Ix i) = return $ v VS.! (fromIntegral i)
offset (SparseVector v) i = 
    case searchFromTo geq 0 (VS.length v - 1) of
        Just j | Assoc i' o <- v VS.! j,
                 i' == i -> return o
        _ -> throw $ IndexNotFound i
    where
    geq j = let Assoc i' _o = (v VS.! j) in i' >= i


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

data Flushed = Flushed

data WEnqueued flushed ref
    = Write !ref !flushed !ByteString
    | Tick

data MMapped = MMapped !StoredOffsets !ByteString

data WriteQueue m  = WriteQueue (Chan m (WEnqueued (MVar m Flushed) (MVar m Ref)))


streamWriteQ :: MonadConc m => WriteQueue m -> Stream (Of (WEnqueued (MVar m Flushed) (MVar m Ref))) m void
streamWriteQ (WriteQueue q) = go
    where
    go = lift (readChan q) >>= yield >> go

newWriteQueueIO :: MonadConc m => Int -> m (WriteQueue m)
newWriteQueueIO _maxLen = WriteQueue <$> newChan

data REnqueued m = Read !Ref !(MVar m ByteString)
                | ReadComplete !Segment !MMapped
                | ReadExn StoredOffsetsDecodeEx
                | SegmentWasGCed !Segment !MMapped -- The new segment (mmapped)

data ReadQueue m = ReadQueue (Chan m (REnqueued m))

class Monad m => MonadEvaluate m where
    evaluateM :: a -> m a

instance MonadEvaluate IO where
    evaluateM = evaluate

readQueue :: MonadConc m => Int -> m (ReadQueue m)
readQueue _maxLen = ReadQueue <$> newChan

storeToQueue :: (Store a, MonadConc m, MonadEvaluate m) => WriteQueue m -> a -> m (Async m (Ref, MVar m Flushed))
storeToQueue (WriteQueue q) value = async $ do
    bs <- evaluateM $ force $ encode $ sized value
    saved <- newEmptyMVar
    flushed <- newEmptyMVar
    writeChan q $ Write saved flushed bs
    ref <- takeMVar saved
    return (ref, flushed)

flushWriteQueue :: MonadConc m => WriteQueue m -> m ()
flushWriteQueue (WriteQueue q) = writeChan q $ Tick


mmap :: (MonadIO m, Throws '[StoredOffsetsDecodeEx] m) => FilePath -> m MMapped
mmap path = do
    bs <- liftIO $ mmapFileByteString path Nothing
    offs <- either throw return $ loadOffsets bs
    return (MMapped offs bs)


-- -- data SegCache = SegCache StoredOffsets (LruCache Ix ByteString)

data ReaderConfig m = ReaderConfig {
        openSeg     :: Segment -> m ByteString,
        lruSize     :: Int,
        maxOpenSegs :: Int
    }

-- Holds all the open segment files (per shard)
data SegmentCache = SegmentCache {
    openSegments :: LruCache Segment (), 
    openSegmentDetails :: Map Segment MMapped
}

emptySegmentCache :: Int -> SegmentCache 
emptySegmentCache maxSize = SegmentCache {openSegments = Lru.empty maxSize, openSegmentDetails = Map.empty}



type family (a::k) ∈ (b::[k]) :: Constraint where
    a ∈ (a ': xs) = ()
    a ∈ (b ': xs) = a ∈ xs

type family Throws (ts::[k]) (m :: * -> *) :: Constraint where
    Throws '[] _ = ()
    Throws (t ': ts) m = (MultiError t m, Throws ts m)


data StoredOffsetsDecodeEx = CacheLengthDecodeEx PeekException | CacheDecodeEx PeekException | SegmentTooShortForCache deriving Show

instance NFData StoredOffsetsDecodeEx where
    rnf (CacheLengthDecodeEx _) = ()
    rnf (CacheDecodeEx _) = ()
    rnf SegmentTooShortForCache = ()
 
loadOffsets :: ByteString -> Either StoredOffsetsDecodeEx StoredOffsets
loadOffsets bs = do
    offsetCacheLen <- either (Left . CacheLengthDecodeEx) Right $ decode $ final 8 
    let offsetCacheLen' = fromIntegral (offsetCacheLen :: Word64)
    if ByteString.length bs < 8 + offsetCacheLen' 
        then Left SegmentTooShortForCache
        else do
            let offsetsBs = ByteString.take offsetCacheLen' (final (offsetCacheLen' + 8)) 
            either (Left . CacheDecodeEx) Right $ decode offsetsBs 
    where
    final n = ByteString.drop (ByteString.length bs - n) bs 
    

updateSegment :: Segment -> MMapped -> SegmentCache -> SegmentCache
updateSegment segment mmapped cache@SegmentCache{..} = 
    cache {openSegmentDetails = Map.adjust (const mmapped) segment openSegmentDetails}

insertSegment :: Segment -> MMapped -> SegmentCache -> SegmentCache
insertSegment segment mmapped SegmentCache{..} =  SegmentCache open' details'
    where
    (removed,open') = Lru.insertView segment () openSegments 
    details' = case removed of
        Nothing -> Map.insert segment mmapped openSegmentDetails 
        Just (key,()) -> Map.insert segment mmapped $ Map.delete key openSegmentDetails 


data Order = Asc | Desc
streamSegFromOffsets :: Throws '[OffsetDecodeEx] m => Order -> MMapped -> Stream (Of (Ix, ByteString)) m ()
streamSegFromOffsets order (MMapped stored bs) = mapM_ load offsets
    where 
    load (ix,off) = do
        val <- lift $ readOff bs off
        yield (ix, val)
    offsets = case order of
        Asc -> inorder
        Desc -> reverse inorder -- Can optimize this whole thing to remove list, if needed
    inorder = case stored of
        OffsetVector v ->  zip (fmap Ix [0..]) (VS.toList v)
        SparseVector v -> fmap (\(Assoc ix off) -> (ix,off)) $ VS.toList v


data OffsetDecodeEx = OffsetDecodeEx PeekException deriving Show

readOff :: Throws '[OffsetDecodeEx] m => ByteString -> Offset -> m ByteString
readOff bs (Offset theOffset) = do
        len :: Word64 <- either (throw . OffsetDecodeEx) return $ decode $ ByteString.take 8 $ ByteString.drop (fromIntegral theOffset) bs 
        return $ ByteString.take (fromIntegral len) $ ByteString.drop (fromIntegral theOffset + 8) bs

readSeg :: (Throws '[IndexError, OffsetDecodeEx] m) => MMapped-> Ix -> m ByteString
readSeg (MMapped offs bs) ix = readOff bs =<< offset offs ix

    

data CacheConsistencyError = SegmentInDetailsButNotLRU Segment deriving Show

readSegCache :: (Throws [IndexError, CacheConsistencyError, OffsetDecodeEx] m) => SegmentCache -> Ref -> m (Maybe (ByteString, SegmentCache))
readSegCache cache@SegmentCache{..} (Ref seg ix) = 
    case Map.lookup seg openSegmentDetails of
        Nothing -> return Nothing
        Just mmapped -> do
            value <- readSeg mmapped ix
            open' <- case Lru.lookup seg openSegments of
                Just ((), new) -> return new
                Nothing -> throw $ SegmentInDetailsButNotLRU seg
            return $ Just (value, cache {openSegments = open'})


data ReaderState m = ReaderState {
    cached :: LruCache Ref ByteString,
    segmentCache :: SegmentCache,
    pending :: Map Segment (Map Ix [MVar m ByteString])
}  

data Found a = NoK1 | NoK2 | Found a

-- There's bound to be a good single-pass way of doing this, but it will take a bit to figure out
appsert :: (Ord k1, Ord k2) => (Maybe a -> a) -> k1 -> k2  -> Map k1 (Map k2 a) -> (Map k1 (Map k2 a), Found a)
appsert f k1 k2 m = (m', found)
    where
    find = maybe NoK1 (maybe NoK2 Found . Map.lookup k2) . Map.lookup k1
    found = find m
    a' = case found of 
        Found a -> f (Just a)
        _ -> f Nothing
    m' = Map.alter g k1 m
    g Nothing = Just (Map.singleton k2 a')
    g (Just inner) = Just (Map.insert k2 a' inner)

data IrrecoverableFailure 
    = IFIndexError IndexError
    | IFCacheConsistencyError CacheConsistencyError
    | IFOffsetDecodeEx OffsetDecodeEx
    | IFReaderConsistencyError ReaderConsistencyError
    | IFStoredOffsetsDecodeEx StoredOffsetsDecodeEx
    | IFInitFailure InitFailure
    | IFSegmentStreamError SegmentStreamError
    | IFGCError GCError
    deriving stock (Typeable, Show)
    deriving anyclass Exception


newtype GCM a = GCM {runGCM :: IO a}
    deriving newtype (Functor,Applicative,Monad,MonadIO)

instance MultiError GCError               GCM where throw = liftIO . throwIO . IFGCError
instance MultiError StoredOffsetsDecodeEx GCM where throw = liftIO . throwIO . IFStoredOffsetsDecodeEx
instance MultiError OffsetDecodeEx        GCM where throw = liftIO . throwIO . IFOffsetDecodeEx


instance MultiError IndexError             IO where throw = liftIO . throwIO . IFIndexError
instance MultiError CacheConsistencyError  IO where throw = liftIO . throwIO . IFCacheConsistencyError
instance MultiError OffsetDecodeEx         IO where throw = liftIO . throwIO . IFOffsetDecodeEx
instance MultiError ReaderConsistencyError IO where throw = liftIO . throwIO . IFReaderConsistencyError
instance MultiError StoredOffsetsDecodeEx  IO where throw = liftIO . throwIO . IFStoredOffsetsDecodeEx
instance MultiError InitFailure            IO where throw = liftIO . throwIO . IFInitFailure
instance MultiError SegmentStreamError     IO where throw = liftIO . throwIO . IFSegmentStreamError

newtype MockDBMT m a = MockDBMT {runMockDBMT :: ExceptT IrrecoverableFailure m a}
    deriving newtype (Functor, Applicative, Monad)

instance Monad m => MultiError IndexError             (MockDBMT m) where throw = MockDBMT . throwError . IFIndexError
instance Monad m => MultiError CacheConsistencyError  (MockDBMT m) where throw = MockDBMT . throwError . IFCacheConsistencyError
instance Monad m => MultiError OffsetDecodeEx         (MockDBMT m) where throw = MockDBMT . throwError . IFOffsetDecodeEx
instance Monad m => MultiError ReaderConsistencyError (MockDBMT m) where throw = MockDBMT . throwError . IFReaderConsistencyError
instance Monad m => MultiError StoredOffsetsDecodeEx  (MockDBMT m) where throw = MockDBMT . throwError . IFStoredOffsetsDecodeEx
instance Monad m => MultiError InitFailure            (MockDBMT m) where throw = MockDBMT . throwError . IFInitFailure
instance Monad m => MultiError SegmentStreamError     (MockDBMT m) where throw = MockDBMT . throwError . IFSegmentStreamError


reader :: (MonadConc m, MonadEvaluate m, Throws '[IndexError, CacheConsistencyError, OffsetDecodeEx, ReaderConsistencyError, StoredOffsetsDecodeEx] m) => ReaderConfig m -> ReadQueue m -> m void
reader ReaderConfig{..} (ReadQueue q) = go initial
    where
    initial = ReaderState (Lru.empty lruSize) (emptySegmentCache maxOpenSegs) Map.empty
    go state = do
        enqueued <- readChan q
        (state', toDispatch) <- readerStep state enqueued
        case toDispatch of
            Nothing -> return ()
            Just seg -> do
                serializer <- async $ do
                    bs <- openSeg seg
                    loaded <- evaluateM $ force $ loadOffsets bs
                    let toSend = case loaded of
                            Right offsets -> ReadComplete seg $ MMapped offsets bs
                            Left exn -> ReadExn exn
                    writeChan q toSend 
                link serializer -- Should be exn-free, but just in case
        go state'
        


data ReaderConsistencyError = ReadCompletedButNoOneCared Segment deriving Show

readerStep :: (MonadConc m, Throws '[IndexError, OffsetDecodeEx, StoredOffsetsDecodeEx, CacheConsistencyError, ReaderConsistencyError] m) => ReaderState m -> REnqueued m -> m (ReaderState m, Maybe Segment)
readerStep state@ReaderState{..} = \case
    Read ref@(Ref seg ix) done -> case Lru.lookup ref cached of
        Just (string, cached') -> do
            putMVar done string
            return (state {cached = cached'}, Nothing)
        Nothing -> do
            cachedRef <- readSegCache segmentCache ref
            case cachedRef of
                Just (string, cache') -> do
                    putMVar done string
                    return (state {segmentCache = cache'}, Nothing)
                Nothing -> do
                    let (pending', waiting) = appsert (maybe [done] (done:)) seg ix pending
                        state' = state {pending = pending'}
                        toDispatch = case waiting of
                            NoK1    -> Just seg
                            NoK2    -> Nothing
                            Found _ -> Nothing
                    return (state', toDispatch)
    ReadComplete  seg mmapped -> 
        let remove _k _v = Nothing in
        case Map.updateLookupWithKey remove seg pending of
            (Nothing, _) -> throw $ ReadCompletedButNoOneCared seg
            (Just waiting, pending') -> do
                let cache' = insertSegment seg mmapped segmentCache
                    clear ix dones = do
                        value <- readSeg mmapped ix 
                        mapM_ (`putMVar` value) dones
                _ <- Map.traverseWithKey clear waiting
                return (state {pending = pending', segmentCache = cache'}, Nothing)
    SegmentWasGCed seg mmapped -> 
        let cache' = updateSegment seg mmapped segmentCache in 
        return (state {segmentCache = cache'}, Nothing)
    ReadExn exn -> throw exn

data FilenameConfig = FilenameConfig {
        segmentPath :: Segment -> FilePath,
        partialSegmentPath :: Segment -> FilePath
    }

data CrashState = Interrupted | Finished

data SegmentResource m resource = SegmentResource {
    writeToSeg :: forall a . Segment -> (resource -> m a) -> m a,
    loadSeg :: CrashState -> Segment -> m (Maybe (m ByteString))
}


data DBConfig m = DBConfig {
        filenameConfig :: FilenameConfig,
        segmentResource :: SegmentResource m Handle,
        maxWriteQueueLen :: Int,
        maxReadQueueLen :: Int,
        readQueueShardShift :: Int,
        readerConfig :: ReaderConfig m,
        consumerLimits :: ConsumerLimits
    }

defaultDBConfig :: FilePath -> DBConfig IO
defaultDBConfig rundir = DBConfig{..}
    where
    segPath seg = rundir ++ "/" ++ show seg
    filenameConfig = FilenameConfig {
            segmentPath = segPath,
            partialSegmentPath = (\seg -> segPath seg ++ "~")
        }
    writeToSeg segment f = undefined
    loadSeg cs segment = do
        exists <- doesFileExist path
        return $ if exists
            then Just $ mmapFileByteString path Nothing
            else Nothing
        where
        path = rundir ++ "/" ++ show segment ++ (case cs of Finished -> ""; Interrupted -> "~") 
    segmentResource = SegmentResource {..}
    maxWriteQueueLen = 16
    maxReadQueueLen = 16
    readQueueShardShift = 0
    readerConfig = ReaderConfig {
            openSeg = (\seg -> (loadSeg Finished seg >>= \case
                Nothing -> error "Segment doesn't exist. TODO: Remove this"
                Just load -> load)),
            lruSize = 16,
            maxOpenSegs = 16
        }
    consumerLimits = ConsumerLimits {
            cutoffCount = 1024,
            cutoffLength = 1024*1024
        }


data DBState m = DBState {
    dbReaders :: ReadCache m,
    dbReaderExn :: Async m Void,
    dbWriter :: WriteQueue m,
    dbWriterExn :: Async m Void
}

dbFinish :: MonadConc m => DBState m -> m Void
dbFinish DBState{..} = snd <$> waitAny [dbReaderExn, dbWriterExn]


loadInitSeg :: FilenameConfig -> InitResult -> IO Segment
loadInitSeg filenameConfig status = case status of
        NoData -> return (Segment 0)
        CleanShutdown highestSeg -> return (succ highestSeg)
        AbruptShutdown partialSeg -> do
            let partialPath = partialSegmentPath filenameConfig partialSeg
                temporaryPath = partialPath ++ ".rebuild"
                finalPath = segmentPath filenameConfig partialSeg
            partial <- liftIO $ mmapFileByteString partialPath Nothing
            hdl <- liftIO $ openFile temporaryPath WriteMode 
            let entries = streamBS partial
                fakeWrite bs = do
                    ref <- liftIO newEmptyMVar
                    flushed <- liftIO newEmptyMVar
                    return $ Write ref flushed bs
                fakeWriteQ = SP.mapM fakeWrite entries
                config = ConsumerConfig partialSeg (const (return ()))
            () :> _savedOffs <- runHandleT (runBufferT $ consumeFile config (hoist lift fakeWriteQ)) hdl
            liftIO $ do
                renameFile temporaryPath finalPath
                removeFile partialPath
            return (succ partialSeg)

setup :: DBConfig IO -> IO (DBState IO)
setup DBConfig{..} = do
    status <- initialize filenameConfig
    initSeg <- loadInitSeg filenameConfig status
    (readers, readersExn) <- liftIO $ spawnReaders maxReadQueueLen readQueueShardShift  readerConfig
    hotCache <- liftIO $ newMVar (initSeg, mempty)
    (writer, writerExn) <- liftIO $ spawnWriter hotCache maxWriteQueueLen initSeg filenameConfig consumerLimits
    return $ DBState (ReadCache readers hotCache) readersExn writer writerExn

data InitFailure = IDon'tUnderstandSearchM deriving Show

data InitResult = NoData | CleanShutdown Segment | AbruptShutdown Segment 

initialize :: (MonadIO m, Throws '[InitFailure] m) => FilenameConfig -> m InitResult
initialize FilenameConfig{..} = do
    let searchRanges = ([0], take 64 $ iterate (*2) 1)
        isThere = liftIO . doesFileExist
        noSegFile ix = not <$> isThere (segmentPath (Segment ix))
    searchM searchRanges divForever noSegFile >>= \case
        [_trivial] -> isThere (partialSegmentPath (Segment 0)) >>= \case
            True -> return $ AbruptShutdown (Segment 0)
            False -> return $ NoData
        [exists,_] -> do
            let highestExists = hiVal exists
            isThere (partialSegmentPath (Segment (highestExists + 1))) >>= \case
                True -> return $ AbruptShutdown (Segment (highestExists + 1))
                False -> return $ CleanShutdown (Segment highestExists)
        _ -> throw IDon'tUnderstandSearchM



-- -- Sharded by segment hash
data Readers m = Readers {shardShift :: Int, 
                          shards :: V.Vector (ReadQueue m)}

readViaReaders :: MonadConc m => Ref -> Readers m -> m (MVar m ByteString)
readViaReaders ref Readers{..} = do
    let segHash = hash (refSegment ref)
        mask = (1 `shiftL` shardShift) - 1
        hash' = segHash .&. mask
        ReadQueue rq = shards V.! hash'
    readVar <- newEmptyMVar
    writeChan rq $ Read ref readVar
    return readVar


spawnWriter :: MVar IO (Segment, Map Offset ByteString) -> Int -> Segment -> FilenameConfig -> ConsumerLimits -> IO (WriteQueue IO, Async IO Void )
spawnWriter hotCache maxWriteQueueLen initSeg filenameConfig consumerLimits = do
    writeQ <- liftIO $ newWriteQueueIO maxWriteQueueLen
    let chunks = hoist lift $ splitWith @IO consumerLimits (streamWriteQ writeQ)
        writeSeg :: Segment -> Stream (Of (WEnqueued (MVar IO Flushed) (MVar IO Ref))) IO x -> IO x
        writeSeg seg writes = do
            let filepath = partialSegmentPath filenameConfig seg
                registerHotcache :: Map Offset ByteString -> IO ()
                registerHotcache offsets = swapMVar @IO hotCache (seg, offsets) >> return ()
                consumeToHdl = consumeFile @(BufferT (HandleT IO)) (ConsumerConfig seg (lift . lift . registerHotcache)) (hoist (lift . lift) writes)
            hdl <- openFile filepath WriteMode
            renameFile filepath (segmentPath filenameConfig seg)
            () :> r <- runHandleT (runBufferT consumeToHdl) hdl
            return r
        writeSegS writes = do
            seg <- get
            put (succ seg)
            liftIO $ writeSeg seg writes
    exn <- async $ flip evalStateT initSeg $ mapsM_ writeSegS chunks
    return (writeQ, exn)

spawnReaders :: (MonadConc m, MonadEvaluate m, Throws '[IndexError, CacheConsistencyError, OffsetDecodeEx, ReaderConsistencyError, StoredOffsetsDecodeEx] m) 
             => Int -> Int -> ReaderConfig m -> m (Readers m, Async m void)
spawnReaders qLen shardShift cfg = do
    let spawn = do
            readQ <- readQueue qLen
            thread <- async $ reader cfg readQ
            return (readQ, thread)
    spawned <- V.replicateM (2 ^ shardShift) spawn 
    let (queues, threads) = V.unzip spawned
    exception <- async $ do
        (_, impossible) <- waitAny (toList threads)
        return impossible
    return (Readers shardShift queues, exception)



segBound :: Segment -> (Ref,Ref)
segBound seg = (Ref seg minBound, Ref seg maxBound)

setRange :: Ord a => a -> a -> Set a -> (Set a, Set a, Set a)
setRange lo hi set = (tooLow, good, tooHigh)
    where
    (tooLow, feasible) = spanAntitone (< lo) set
    (good, tooHigh) = spanAntitone (<= hi) feasible

-- Whenever the writer finishes a segment, it takes a snapshot of all active roots (which can only shrink
-- wrt that segment) and passes it to the GC
data GcSnapshot = GcSnapshot {snapshotSegment :: Segment, snapshotRoots :: Set Ref}

spawnGcManager :: GcConfig ByteString -> FilenameConfig -> Chan IO GcSnapshot -> (Segment -> MMapped -> IO ()) -> GCM void
spawnGcManager gcconf filenameConfig completeSegs registerGC = forever (liftIO (readChan completeSegs) >>= peristaltize)
    where
    peristaltize GcSnapshot{..} | not (null newerRefs) = error "Roots persist from older segments"
                                | null currentIxes = undefined "Just save an empty segment"
                                | otherwise = do
                                    let old_filepath = segmentPath filenameConfig snapshotSegment
                                    old_mmapped <- mmap old_filepath
                                    let collect = gc @(BufferT (HandleT GCM)) gcconf snapshotSegment currentIxes RootKnown old_mmapped
                                        new_filepath = partialSegmentPath filenameConfig snapshotSegment
                                    new_hdl <- liftIO $ openFile new_filepath WriteMode 
                                    (_offs, activeRefs, _perst) <- runHandleT (runBufferT collect) new_hdl
                                    new_mmapped <- mmap new_filepath
                                    liftIO $ registerGC snapshotSegment new_mmapped
                                    let olderRefs' = union olderRefs activeRefs
                                    score <- liftIO $ getRandomR (0.0, 1.0 :: Double)
                                    if score < 0.1 || snapshotSegment == minBound
                                        then return ()
                                        else peristaltize (GcSnapshot {snapshotRoots = olderRefs', snapshotSegment = pred snapshotSegment})

        where
        (olderRefs, currentRefs, newerRefs) = let (lo,hi) = segBound snapshotSegment in setRange lo hi snapshotRoots
        currentIxes = Set.map refIx currentRefs


data ReadCache m = ReadCache {
        readers :: Readers m,
        -- Everyone has to read this for every read. Seems bad. Weak IORef MM causes issues, must use something with strong MM (like MVar)
        hot :: MVar m (Segment, Map Offset ByteString)
    }


readViaReadCache :: MonadConc m => Ref -> ReadCache m -> m (Async m ByteString)
readViaReadCache ref ReadCache{..} = async $ do
    (hotSegment, hotcache) <- readMVar hot
    if hotSegment == refSegment ref
        then return $ snd (Map.elemAt (fromEnum $ refIx ref) hotcache)
        else do
            readVar <- readViaReaders ref readers
            takeMVar readVar

data ConsumerLimits = ConsumerLimits {
        cutoffCount :: Int,
        cutoffLength :: Offset
    }

exceeds :: (Int, Offset) -> ConsumerLimits -> Bool
(count,len) `exceeds` ConsumerLimits{..} = count >= cutoffCount || len >= cutoffLength


splitAfter :: forall x a m r . Monad m => (x -> a -> x) -> x -> (x -> Bool) -> Stream (Of a) m r -> Stream (Stream (Of a) m) m r
splitAfter step initial predicate = go
    where
    go = wrap . segment initial
    segment x stream = lift (next stream) >>= \case
        Left r -> return (return r)
        Right (a, stream') -> do
            yield a
            let x' = step x a
            if predicate x'
                then return (go stream')
                else segment x' stream'

splitWith :: forall m flushed ref a . Monad m
          => ConsumerLimits 
          -> Stream (Of (WEnqueued flushed ref)) m a 
          -> Stream (Stream (Of (WEnqueued flushed ref)) m) m a
splitWith limits = splitAfter throughput (0,0) (`exceeds` limits) 
    where
    throughput (count,len) (Write _ _ bs) = (count + 1, len `plus` ByteString.length bs)
    throughput tp          Tick           = tp




data Persistence = NormalGC
                 | PersistentRoot deriving (Show)

data NoParse = NoParse String deriving Show

newtype GcConfig entry = GcConfig {
    inspect :: entry -> Either NoParse (VS.Vector Ref, Persistence)
}


data PersistenceStatus = RootKnown
                       | RootUnknown deriving (Show)

data SegGcState = SegGcState {
    liveHere :: Set Ix,
    liveThere :: Set Ref,
    persistenceStatus :: PersistenceStatus
}





class Monad m => Writable m where
    writeM :: ByteString -> m ()
    writeMB :: Builder -> m ()
    flushM :: m ()

newtype HandleT m a = HandleT (ReaderT Handle m a) 
    deriving newtype (Functor, Applicative, Monad, MonadIO, MonadTrans, MonadConc, MonadThrow, MonadCatch, MonadMask)

runHandleT :: HandleT m a -> Handle -> m a
runHandleT (HandleT reader) handle = runReaderT reader handle

instance MonadIO m => Writable (HandleT m) where
    {-# INLINE writeM #-}
    writeM bs = HandleT $ ask >>= \hdl -> liftIO (ByteString.hPut hdl bs)
    {-# INLINE writeMB #-}
    writeMB bd = HandleT $ ask >>= \hdl -> liftIO (hPutBuilder hdl bd)
    flushM = HandleT $ ask >>= \hdl -> liftIO (hFlush hdl)

instance MultiError e m => MultiError e (HandleT m) where
    throw = lift . throw


newtype BufferT m a = BufferT (StateT Builder m a)
    deriving newtype (Functor, Applicative, Monad, MonadIO, MonadTrans, MonadConc, MonadThrow, MonadCatch, MonadMask)

instance Writable m => Writable (BufferT m) where
    {-# INLINE writeM #-}
    writeM bs = BufferT $ modify (<> byteString bs)
    {-# INLINE writeMB #-}
    writeMB bd = BufferT $ modify (<> bd)
    flushM = BufferT $ do
        builder <- get
        lift $ writeMB builder
        lift $ flushM
        put mempty

instance MultiError e m => MultiError e (BufferT m) where
    throw = lift . throw

runBufferT :: Writable m => BufferT m a -> m a
runBufferT (BufferT state) = do
        (a,builder) <- runStateT state mempty
        writeMB builder
        return a

newtype DummyFileT m a = DummyFileT (StateT Builder m a)
    deriving newtype (Functor, Applicative, Monad, MonadIO, MonadTrans)

instance Monad m => Writable (DummyFileT m) where
    {-# INLINE writeM #-}
    writeM bs = DummyFileT $ modify (<> byteString bs)
    {-# INLINE writeMB #-}
    writeMB bd = DummyFileT $ modify (<> bd)
    flushM = return ()

runDummyFileT :: DummyFileT m a -> m (a, Builder)
runDummyFileT (DummyFileT state) = runStateT state mempty

gc :: (Throws '[GCError, OffsetDecodeEx] m, Writable m) => GcConfig ByteString -> Segment -> Set Ix -> PersistenceStatus -> MMapped -> m (StoredOffsets, Set Ref, PersistenceStatus)
gc cfg thisSegment live perst = fmap unwrap . repackFile . gc' cfg thisSegment live perst . streamSegFromOffsets Desc
    where 
    unwrap (offs :> (live',perst') :> ()) = (offs, live', perst')


data GCError
    = GCDeserializeError 
    | SegmentCausalityViolation Segment Ref 
    | IndexCausalityViolation Ix Ref 
    | GCParseError NoParse
    deriving Show


gc' :: Throws '[GCError] m => GcConfig entry -> Segment -> Set Ix -> PersistenceStatus -> Stream (Of (Ix, entry)) m o -> Stream (Of (Ix, entry)) m (Of (Set Ref, PersistenceStatus) o)
gc' GcConfig{..} thisSegment here perst = foldM include (return (SegGcState here Set.empty perst)) (return . finish) . hoist lift
    where
    finish SegGcState{..} = (liveThere, persistenceStatus)
    include state (ix,bs) = do
        (children, persistence) <- either (lift . throw . GCParseError) return $ inspect bs
        let normal = if ix `Set.member` (liveHere state) 
                then do
                    yield (ix,bs)
                    includeChildren state ix children
                else return state
        case persistence of
            NormalGC -> normal
            PersistentRoot -> case persistenceStatus state of
                 RootKnown -> normal
                 RootUnknown -> do
                    yield (ix,bs)
                    includeChildren (state {persistenceStatus = RootKnown}) ix children
    includeChildren state thisIx children = VS.foldM' step state children
        where
        step s@SegGcState{..} ref@(Ref seg ix)
            | seg > thisSegment = lift $ throw $ SegmentCausalityViolation thisSegment ref 
            | seg == thisSegment && ix >= thisIx = lift $ throw $ IndexCausalityViolation thisIx ref 
            | seg == thisSegment = return $  s {liveHere = Set.insert ix liveHere}
            | otherwise = return $ s {liveThere = Set.insert ref liveThere}


data RepackState = RepackState {
        repackOffset :: Offset,
        repackMap :: Map Ix Offset
    }

repackFile :: Writable m => Stream (Of (Ix, ByteString)) m r -> m (Of StoredOffsets r)
repackFile = foldM step (return initial) finalize
    where
    step RepackState{..} (ix,bs) = do
        writeM bs
        return (RepackState {repackOffset = repackOffset `plus` ByteString.length bs,
                             repackMap = Map.insert ix repackOffset repackMap})
    finalize RepackState{..} = do
        let offsets = SparseVector $ VS.fromList $ fmap (uncurry Assoc) $ Map.toList repackMap
        writeOffsetsTo offsets
        return offsets
    initial = RepackState {repackOffset = Offset 0, repackMap = Map.empty}


data ConsumerConfig m = ConsumerConfig {
        segment :: Segment,
        register :: Map Offset ByteString -> m () -- Should save in "hot" of ReadCache
    }

data ConsumerState flushed = ConsumerState {
        entries :: Map Offset ByteString,
        writeOffset :: Offset,
        flushQueue :: [flushed]
    }


data SegmentStep = Incomplete | Finalizer ByteString | Packet ByteString ByteString




segmentStep :: ByteString -> SegmentStep
segmentStep bs = 
    if | totalLen < 8 -> Incomplete
       | packetLen == maxBound -> Finalizer afterLen
       | ByteString.length bs < 8 + packetLen' -> Incomplete
       | otherwise -> Packet packet remainder
    where
    totalLen = ByteString.length bs
    packetLen :: Word64 = decodeEx $ ByteString.take 8 bs
    packetLen' :: Int = fromIntegral packetLen
    afterLen = ByteString.drop 8 bs
    (packet,remainder) = ByteString.splitAt packetLen' afterLen


data SegmentStreamError = RepeatedFinalizer deriving Show

-- -- no fundep m -> e
class Monad m => MultiError e m where
    throw :: e -> m a


streamBS :: Throws '[SegmentStreamError] m => ByteString -> Stream (Of ByteString) m (Maybe ByteString)
streamBS = go
    where
    go bs = case segmentStep bs of
        Incomplete -> return Nothing
        Finalizer rest -> case segmentStep rest of
            Packet packet _ -> return (Just packet)
            Incomplete -> return Nothing
            Finalizer _ -> lift $ throw RepeatedFinalizer
        Packet packet rest -> do
            yield packet
            go rest

data ConsumerAction flushed ref = Flush [flushed] | WriteToLog ByteString (Map Offset ByteString) ref Ix | Nop


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

{-# INLINE consumeFile #-}
consumeFile :: forall m r . (MonadConc m, Writable m) => ConsumerConfig m -> Stream (Of (WEnqueued (MVar m Flushed) (MVar m Ref))) m r -> m (Of () r)
consumeFile cfg = consume (saveFile cfg) (finalizeFile cfg)


{-# INLINE saveFile #-}
saveFile :: (MonadConc m, Writable m) => ConsumerConfig m -> ConsumerAction (MVar m Flushed) (MVar m Ref) -> m ()
saveFile ConsumerConfig{..} action = case action of
        Flush flushed -> do
            flushM
            mapM_ (`putMVar` Flushed) flushed
        WriteToLog bs entries refVar ix -> do
            writeM bs
            -- This is a bit confusing: register is supposed to update the shared "hot" cache for the active write head.
            -- We *must* make a globally visible update to the hot cache before returning the ref, or else
            -- someone could try to do a read that would fail.
            -- Can't use IORef - weak memory model. MVar should be fine. STM/MVar interaction OK?
            register entries
            putMVar refVar (Ref segment ix) 
        Nop -> return ()

writeOffsetsTo :: Writable m => StoredOffsets -> m ()
writeOffsetsTo offsets = do
    let storedOffsets = encode offsets  
        offsetsLen = fromIntegral (ByteString.length storedOffsets) :: Word64 
    writeM $ encode (maxBound :: Word64)
    writeM $ encode offsetsLen
    writeM storedOffsets -- length on either side so we can read from the end of file or in order
    writeM $ encode offsetsLen
    flushM

finalizeFile :: (MonadConc m, Writable m) => ConsumerConfig m -> ConsumerState (MVar m Flushed) -> m ()
finalizeFile ConsumerConfig{..} ConsumerState{..} = do
    flushM
    mapM_ (`putMVar` Flushed) flushQueue
    let offsets = OffsetVector (VS.fromList (keys entries))
    writeOffsetsTo offsets

-- demo :: IO () 
-- demo = do
--     let cfg = defaultDBConfig "/tmp/rundir"
--     state <- runDBM (setup cfg)
--     let wq = dbWriter state
--     writes <- replicateM (8*1024) (storeToQueue wq (ByteString.replicate (16*4096) 0x44))
--     (refs,flushes) <- unzip <$> mapM wait writes
--     flushWriteQueue wq
--     mapM_ takeMVar flushes
--     print $ length refs




