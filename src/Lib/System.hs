module Lib.System  where

import           Data.Store (Store, encode, decode, decodeEx, PeekException) 
import           Control.Concurrent.Async (Async, async, link, waitAny, wait)
-- import           Control.Concurrent (forkIO)
import           Control.Concurrent.MVar (MVar, newEmptyMVar, takeMVar, putMVar)
import           Control.Concurrent.STM.TVar (TVar)
import           Control.DeepSeq (NFData, rnf, force)
import           Control.Exception (Exception,evaluate, throwIO)
import           Control.Concurrent.STM.TBMChan (TBMChan, writeTBMChan, newTBMChanIO, readTBMChan)
import           Control.Concurrent.STM.TBChan (TBChan, writeTBChan, readTBChan, newTBChanIO)
import           Control.Concurrent.STM (atomically)
import           Data.Map.Strict as Map (Map, insert, keys, empty, alter, updateLookupWithKey, adjust, delete, lookup, singleton, traverseWithKey)
import qualified Data.Map.Strict as Map (toList)
import           Data.Set as Set (Set, insert, member, empty)
import qualified Data.ByteString as ByteString
import           Data.ByteString (ByteString)
import           Data.ByteString.Builder (Builder, byteString)
import           Data.Foldable (toList)
import           System.IO (Handle, hFlush, IOMode(AppendMode), openFile)
import           System.IO.MMap (mmapFileByteString)
import           System.Directory (doesFileExist, renameFile, removeFile)
import           Data.Word (Word64)
import           Lib.StoreStream (sized)
import qualified Data.Vector.Storable as VS
import qualified Data.Vector as V
import           Data.LruCache as Lru (LruCache, empty, lookup, insertView)
import           Data.Hashable (Hashable)
import           GHC.Generics (Generic)
import           Foreign.Ptr (Ptr, plusPtr, castPtr)
import           Foreign.Storable (Storable, peek, poke, sizeOf, alignment)
import           Streaming.Prelude (Stream, yield, foldM, next)
import qualified Streaming.Prelude as SP (mapM)
import           Streaming (wrap, mapsM_)
import           Data.Functor.Of (Of((:>)))
import           Control.Monad.Trans.Class (lift)
import           Control.Monad.Morph (hoist)
import           Numeric.Search.Range (searchFromTo)
import           Numeric.Search (searchM, divForever, hiVal)
-- import           Control.Monad.Error.Class (MonadError, throwError)
import           Control.Monad.IO.Class (MonadIO, liftIO)
import           Control.Monad.Except (ExceptT, throwError)
import           GHC.Exts (Constraint)
import           Type.Reflection (Typeable)
import           Data.Void (Void, absurd)
import           Control.Monad.Reader (ReaderT(..), ask)
import           Control.Monad.State.Strict (StateT(..), modify, get, put, execStateT)
import           Control.Monad.Identity (Identity(..))
import           Control.Monad (replicateM)
import           Data.Proxy (Proxy(Proxy))

newtype Segment = Segment Word64
    deriving newtype (Eq,Ord,Hashable, Store, Storable, Show, Enum)

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

data WriteQueue = WriteQueue (TBMChan (WEnqueued (MVar Flushed) (MVar Ref)))

streamWriteQ :: MonadIO m => WriteQueue -> Stream (Of (WEnqueued (MVar Flushed) (MVar Ref))) m ()
streamWriteQ (WriteQueue q) = go
    where
    go = (liftIO $ atomically $ readTBMChan q) >>= \case
        Nothing -> return ()
        Just thing -> yield thing >> go

newWriteQueueIO :: Int -> IO WriteQueue
newWriteQueueIO maxLen = WriteQueue <$> newTBMChanIO maxLen

data REnqueued = Read !Ref !(MVar ByteString)
               | ReadComplete !Segment !MMapped
               | ReadExn StoredOffsetsDecodeEx
               | SegmentWasGCed !Segment !MMapped -- The new segment (mmapped)

data ReadQueue = ReadQueue (TBChan REnqueued)

readQueue :: Int -> IO ReadQueue
readQueue maxLen = ReadQueue <$> newTBChanIO maxLen

storeToQueue :: Store a => WriteQueue -> a -> IO (Async (Ref, MVar Flushed))
storeToQueue (WriteQueue q) value = async $ do
    bs <- evaluate $ force $ encode $ sized value
    saved <- newEmptyMVar
    flushed <- newEmptyMVar
    atomically $ writeTBMChan q $ Write saved flushed bs
    ref <- takeMVar saved
    return (ref, flushed)

flushWriteQueue :: WriteQueue -> IO ()
flushWriteQueue (WriteQueue q) = atomically $ writeTBMChan q $ Tick


mmap :: (MonadIO m, Throws '[StoredOffsetsDecodeEx] m) => FilePath -> m MMapped
mmap path = do
    bs <- liftIO $ mmapFileByteString path Nothing
    offs <- either throw return $ loadOffsets bs
    return (MMapped offs bs)


-- data SegCache = SegCache StoredOffsets (LruCache Ix ByteString)

data ReaderConfig = ReaderConfig {
        openSeg     :: Segment -> IO ByteString,
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


data ReaderState = ReaderState {
    cached :: LruCache Ref ByteString,
    segmentCache :: SegmentCache,
    pending :: Map Segment (Map Ix [MVar ByteString])
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
    deriving stock (Typeable, Show)
    deriving anyclass Exception

newtype DBM a = DBM {runDBM :: IO a}
    deriving newtype (Functor,Applicative,Monad,MonadIO)

instance MultiError IndexError             DBM where throw = liftIO . throwIO . IFIndexError
instance MultiError CacheConsistencyError  DBM where throw = liftIO . throwIO . IFCacheConsistencyError
instance MultiError OffsetDecodeEx         DBM where throw = liftIO . throwIO . IFOffsetDecodeEx
instance MultiError ReaderConsistencyError DBM where throw = liftIO . throwIO . IFReaderConsistencyError
instance MultiError StoredOffsetsDecodeEx  DBM where throw = liftIO . throwIO . IFStoredOffsetsDecodeEx
instance MultiError InitFailure            DBM where throw = liftIO . throwIO . IFInitFailure
instance MultiError SegmentStreamError     DBM where throw = liftIO . throwIO . IFSegmentStreamError

newtype MockDBMT m a = MockDBMT {runMockDBMT :: ExceptT IrrecoverableFailure m a}
    deriving newtype (Functor, Applicative, Monad)

instance Monad m => MultiError IndexError             (MockDBMT m) where throw = MockDBMT . throwError . IFIndexError
instance Monad m => MultiError CacheConsistencyError  (MockDBMT m) where throw = MockDBMT . throwError . IFCacheConsistencyError
instance Monad m => MultiError OffsetDecodeEx         (MockDBMT m) where throw = MockDBMT . throwError . IFOffsetDecodeEx
instance Monad m => MultiError ReaderConsistencyError (MockDBMT m) where throw = MockDBMT . throwError . IFReaderConsistencyError
instance Monad m => MultiError StoredOffsetsDecodeEx  (MockDBMT m) where throw = MockDBMT . throwError . IFStoredOffsetsDecodeEx
instance Monad m => MultiError InitFailure            (MockDBMT m) where throw = MockDBMT . throwError . IFInitFailure
instance Monad m => MultiError SegmentStreamError     (MockDBMT m) where throw = MockDBMT . throwError . IFSegmentStreamError


reader :: (MonadIO m, Throws '[IndexError, CacheConsistencyError, OffsetDecodeEx, ReaderConsistencyError, StoredOffsetsDecodeEx] m) => ReaderConfig -> ReadQueue -> m void
reader ReaderConfig{..} (ReadQueue q) = go initial
    where
    initial = ReaderState (Lru.empty lruSize) (emptySegmentCache maxOpenSegs) Map.empty
    go state = do
        enqueued <- liftIO $ atomically $ readTBChan q
        (state', toDispatch) <- readerStep state enqueued
        case toDispatch of
            Nothing -> return ()
            Just seg -> liftIO $ do
                serializer <- async $ do
                    bs <- openSeg seg
                    loaded <- evaluate $ force $ loadOffsets bs
                    let toSend = case loaded of
                            Right offsets -> ReadComplete seg $ MMapped offsets bs
                            Left exn -> ReadExn exn
                    atomically $ writeTBChan q toSend 
                link serializer -- Should be exn-free, but just in case
        go state'
        


data ReaderConsistencyError = ReadCompletedButNoOneCared Segment deriving Show

readerStep :: (MonadIO m, Throws '[IndexError, OffsetDecodeEx, StoredOffsetsDecodeEx, CacheConsistencyError, ReaderConsistencyError] m) => ReaderState -> REnqueued -> m (ReaderState, Maybe Segment)
readerStep state@ReaderState{..} = \case
    Read ref@(Ref seg ix) done -> case Lru.lookup ref cached of
        Just (string, cached') -> do
            liftIO $ putMVar done string
            return (state {cached = cached'}, Nothing)
        Nothing -> do
            cachedRef <- readSegCache segmentCache ref
            case cachedRef of
                Just (string, cache') -> do
                    liftIO $ putMVar done string
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
                        mapM_ (\done -> liftIO $ putMVar done value) dones
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


data DBConfig = DBConfig {
        filenameConfig :: FilenameConfig,
        maxWriteQueueLen :: Int,
        maxReadQueueLen :: Int,
        readQueueShardShift :: Int,
        readerConfig :: ReaderConfig,
        consumerLimits :: ConsumerLimits
    }

defaultDBConfig :: FilePath -> DBConfig
defaultDBConfig rundir = DBConfig{..}
    where
    segPath seg = rundir ++ "/" ++ show seg
    filenameConfig = FilenameConfig {
            segmentPath = segPath,
            partialSegmentPath = (\seg -> segPath seg ++ "~")
        }
    maxWriteQueueLen = 16
    maxReadQueueLen = 16
    readQueueShardShift = 0
    readerConfig = ReaderConfig {
            openSeg = (\seg -> mmapFileByteString (segPath seg) Nothing),
            lruSize = 16,
            maxOpenSegs = 16
        }
    consumerLimits = ConsumerLimits {
            cutoffCount = 1024,
            cutoffLength = 1024*1024
        }


data DBState = DBState {
    dbReaders :: Readers,
    dbReaderExn :: Async Void,
    dbWriter :: WriteQueue,
    dbWriterExn :: Async Segment
}

dbFinish :: DBState -> IO Segment
dbFinish DBState{..} = snd <$> waitAny [fmap absurd dbReaderExn, dbWriterExn]


loadInitSeg :: FilenameConfig -> InitResult -> DBM Segment
loadInitSeg filenameConfig status = case status of
        NoData -> return (Segment 0)
        CleanShutdown highestSeg -> return (succ highestSeg)
        AbruptShutdown partialSeg -> do
            let partialPath = partialSegmentPath filenameConfig partialSeg
                temporaryPath = partialPath ++ ".rebuild"
                finalPath = segmentPath filenameConfig partialSeg
            partial <- liftIO $ mmapFileByteString partialPath Nothing
            hdl <- liftIO $ openFile temporaryPath AppendMode 
            let entries = streamBS partial
                fakeWrite bs = do
                    ref <- liftIO newEmptyMVar
                    flushed <- liftIO newEmptyMVar
                    return $ Write ref flushed bs
                fakeWriteQ = SP.mapM fakeWrite entries
                config = ConsumerConfig partialSeg (const (return ()))
            () :> _savedOffs <- runReaderT (consumeFile config (hoist lift fakeWriteQ)) hdl
            liftIO $ do
                renameFile temporaryPath finalPath
                removeFile partialPath
            return (succ partialSeg)

setup :: DBConfig -> DBM DBState
setup DBConfig{..} = do
    status <- initialize filenameConfig
    initSeg <- loadInitSeg filenameConfig status
    (readers, readersExn) <- liftIO $ spawnReaders maxReadQueueLen readQueueShardShift  readerConfig
    (writer, writerExn) <- liftIO $ spawnWriter maxWriteQueueLen initSeg filenameConfig consumerLimits
    return $ DBState readers readersExn writer writerExn

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


data CoreState = CoreState {
        coreCollector :: TVar GcState,
        coreReaders :: ReadCache
    }

data GcState = GcState {
        roots :: Set Ref,
        persistent :: Ref
    }

-- Sharded by segment hash
data Readers = Readers {shardShift :: Int, 
                        shards :: V.Vector ReadQueue}


spawnWriter :: Int -> Segment -> FilenameConfig -> ConsumerLimits -> IO (WriteQueue, Async Segment )
spawnWriter maxWriteQueueLen initSeg filenameConfig consumerLimits = do
    writeQ <- liftIO $ newWriteQueueIO maxWriteQueueLen
    let chunks = hoist lift $ splitWith @IO consumerLimits (streamWriteQ writeQ)
        writeSeg :: Segment -> Stream (Of (WEnqueued (MVar Flushed) (MVar Ref))) IO x -> IO x
        writeSeg seg writes = do
            let filepath = partialSegmentPath filenameConfig seg
            hdl <- openFile filepath AppendMode
            let registerHotcache _offsets = return () -- NB TODO: Actually use the ReadCache
                consumeToHdl = consumeFile @(ReaderT Handle IO) (ConsumerConfig seg registerHotcache) (hoist lift writes)
            renameFile filepath (segmentPath filenameConfig seg)
            () :> r <- runReaderT consumeToHdl hdl
            return r
        writeSegS writes = do
            seg <- get
            put (succ seg)
            liftIO $ writeSeg seg writes
    exn <- async $ flip execStateT initSeg $ mapsM_ writeSegS chunks
    return (writeQ, exn)

spawnReaders :: Int -> Int -> ReaderConfig -> IO (Readers, Async exception)
spawnReaders qLen shardShift cfg = do
    let spawn = do
            readQ <- readQueue qLen
            thread <- async $ runDBM $ reader cfg readQ
            return (readQ, thread)
    spawned <- V.replicateM (2 ^ shardShift) spawn 
    let (queues, threads) = V.unzip spawned
    exception <- async $ do
        (_, impossible) <- waitAny (toList threads)
        return impossible
    return (Readers shardShift queues, exception)

-- spawnGcManager :: 


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
    flushM :: m ()

instance MonadIO m => Writable (ReaderT Handle m) where
    writeM bs = ask >>= \hdl -> liftIO (ByteString.hPut hdl bs)
    flushM = ask >>= \hdl -> liftIO (hFlush hdl)

instance Monad m => Writable (StateT Builder m) where
    writeM bs = modify (<> byteString bs)
    flushM = return ()


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

-- no fundep m -> e
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


consumeFile :: forall m var r . (MonadVar var m, Writable m) => ConsumerConfig m -> Stream (Of (WEnqueued (var Flushed) (var Ref))) m r -> m (Of () r)
consumeFile cfg = consume (saveFile cfg) (finalizeFile cfg)


class MonadVar var m where
    putVar :: var a -> a -> m ()
    newEmptyVar :: m (var a)

instance MonadIO m => MonadVar MVar m where
    putVar var = liftIO . putMVar var
    newEmptyVar = liftIO newEmptyMVar

instance MonadVar Proxy Identity where
    putVar _ _ = return ()
    newEmptyVar = return Proxy

saveFile :: (MonadVar var m, Writable m) => ConsumerConfig m -> ConsumerAction (var Flushed) (var Ref) -> m ()
saveFile ConsumerConfig{..} action = case action of
        Flush flushed -> do
            flushM
            mapM_ (`putVar` Flushed) flushed
        WriteToLog bs entries refVar ix -> do
            writeM bs
            -- This is a bit confusing: register is supposed to update the shared "hot" cache for the active write head.
            -- We *must* make a globally visible update to the hot cache before returning the ref, or else
            -- someone could try to do a read that would fail.
            -- Can't use IORef - weak memory model. MVar should be fine. STM/MVar interaction OK?
            register entries
            putVar refVar (Ref segment ix) 
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

finalizeFile :: (MonadVar var m, Writable m) => ConsumerConfig m -> ConsumerState (var Flushed) -> m ()
finalizeFile ConsumerConfig{..} ConsumerState{..} = do
    flushM
    mapM_ (`putVar` Flushed) flushQueue
    let offsets = OffsetVector (VS.fromList (keys entries))
    writeOffsetsTo offsets

demo :: IO () 
demo = do
    let cfg = defaultDBConfig "/tmp/rundir"
    state <- runDBM (setup cfg)
    let wq = dbWriter state
    writes <- replicateM (16*1024) (storeToQueue wq (ByteString.replicate 4096 0x44))
    (refs,flushes) <- unzip <$> mapM wait writes
    flushWriteQueue wq
    mapM_ takeMVar flushes
    print $ length refs



