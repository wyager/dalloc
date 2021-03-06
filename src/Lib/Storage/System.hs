{-# LANGUAGE UndecidableInstances #-} -- So that we can GND-derive MonadConc instances for transformers 
{-# LANGUAGE RoleAnnotations #-}  
module Lib.Storage.System (
    demoIO2
  , test
  , DBT(..)
  , NoGC
  , MonadEvaluate
  , DBState
  , FakeHandle
  , MockDBMT
  , DBConfig
  , readViaReadCache
  , flushWriteQueue
  , dbWriterExn
  , dbReaderExn
  , storeToQueue
  , dbReaders
  , dbWriter
  , mockDBConfig
  , defaultDBConfig
  , setupIO
  , Reft
    )  where

import           Data.Store (Store, encode, decode, decodeEx, PeekException) 
import qualified Data.Store as Store
import           Control.Concurrent.Classy.Async (Async, async, link, waitAny, wait)
import           Control.Concurrent.Classy.MVar (MVar, newEmptyMVar, takeMVar, putMVar, swapMVar, readMVar, newMVar, modifyMVar)
import           Control.DeepSeq (NFData, rnf, force)
import           Control.Exception (Exception, evaluate)
import           Control.Concurrent.Classy.Chan (Chan, newChan, readChan, writeChan)
import           Data.Map.Strict as Map (Map, member, insert, insertWith, alterF, keys, empty, alter, updateLookupWithKey, adjust, delete, lookup, singleton, traverseWithKey, elemAt)
import qualified Data.Map.Strict as Map (toList, lookupMax)
import           Data.Set as Set (Set, insert, member, empty, union, spanAntitone, singleton)
import qualified Data.Set as Set (map)
import qualified Data.ByteString as ByteString
import           Data.ByteString (ByteString)
import           Data.ByteString.Lazy (toStrict)
import           Data.ByteString.Builder (Builder, byteString, hPutBuilder, toLazyByteString, lazyByteString)
import           Data.Foldable (toList)
import           System.IO (Handle, hFlush, IOMode(WriteMode), withFile)
import           System.IO.MMap (mmapFileByteString)
import           System.Directory (doesFileExist, renameFile, removeFile)
import           Data.Word (Word64)
import qualified Data.Vector.Storable as VS
import qualified Data.Vector.Unboxed as VU
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
import           Control.Monad.IO.Class (MonadIO, liftIO)
import           GHC.Exts (Constraint)
import           Type.Reflection (Typeable)
import           Data.Void (Void, absurd)
import           Control.Monad.Reader (ReaderT(..), ask, MonadReader)
import           Control.Monad.State.Strict (StateT(..), modify, get, put, evalStateT)
import           Control.Monad (replicateM, forever)
import           Data.Bits ((.&.), shiftL)
import           Control.Concurrent.Classy (MonadConc)
import           Control.Monad.Catch (MonadThrow, MonadCatch, MonadMask, throwM)
import           Numeric.Search.Range (searchFromTo)
import           System.Random (RandomGen, randomR)
import           Data.Tuple (swap)
import           Data.Proxy (Proxy(Proxy))
import qualified Lib.Structures.GiST as G
import qualified Lib.Structures.GiST.Example as Gex
import Data.Functor.Contravariant (contramap)

-- import           Data.Coerce (coerce)

newtype Segment = Segment Word64
    deriving newtype (Eq,Ord,Hashable, Store, Storable, Show, Enum, Bounded, Num, Real, Integral)

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

data IsRoot = YesRoot | NotRoot

data WEnqueued var gc
    = SeshInit !(var (Initial gc))
    | SeshDone !(Final gc)
    | Write !(var Ref) !(var Flushed) !ByteString !(WriteData gc)
    | Tick


data MMapped = MMapped !StoredOffsets !ByteString

data WriteQueue m gc = WriteQueue (Chan m (WEnqueued (MVar m) gc))


streamWriteQ :: MonadConc m => WriteQueue m gc -> Stream (Of (WEnqueued (MVar m) gc)) m void
streamWriteQ (WriteQueue q) = go
    where
    go = lift (readChan q) >>= yield >> go

newWriteQueue :: MonadConc m => Int -> m (WriteQueue m gc)
newWriteQueue _maxLen = WriteQueue <$> newChan

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

storeToQueue :: (MonadConc m, MonadEvaluate m) => WriteQueue m gc -> ByteString -> WriteData gc -> m (Async m (Ref, MVar m Flushed))
storeToQueue (WriteQueue q) bs writeData = async $ do
    saved <- newEmptyMVar
    flushed <- newEmptyMVar
    writeChan q $ Write saved flushed bs writeData
    ref <- takeMVar saved
    return (ref, flushed)

flushWriteQueue :: MonadConc m => WriteQueue m gc -> m ()
flushWriteQueue (WriteQueue q) = writeChan q $ Tick

data ReaderConfig = ReaderConfig {
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
    | IFSegmentStreamError SegmentStreamError
    | IFGCError GCError
    | IFSegmentNotFoundEx SegmentNotFoundEx
    | IFNoParse NoParse
    deriving stock (Typeable, Show)
    deriving anyclass Exception



instance MultiError GCError               IO where throw = throwM . IFGCError

instance MultiError IndexError             IO where throw = throwM . IFIndexError
instance MultiError CacheConsistencyError  IO where throw = throwM . IFCacheConsistencyError
instance MultiError OffsetDecodeEx         IO where throw = throwM . IFOffsetDecodeEx
instance MultiError ReaderConsistencyError IO where throw = throwM . IFReaderConsistencyError
instance MultiError StoredOffsetsDecodeEx  IO where throw = throwM . IFStoredOffsetsDecodeEx
instance MultiError SegmentStreamError     IO where throw = throwM . IFSegmentStreamError
instance MultiError SegmentNotFoundEx      IO where throw = throwM . IFSegmentNotFoundEx
instance MultiError NoParse                IO where throw = throwM . IFNoParse


mockThrow :: Monad m => IrrecoverableFailure -> MockDBMT m a
mockThrow failure = MockDBMT $ \ mThrow _fs -> fmap absurd $ mThrow failure

 -- fmap absurd . MockDBMT . lift $ ask >>= \mThrow -> lift (mThrow failure)

instance Monad m => MultiError IndexError             (MockDBMT m) where throw = mockThrow . IFIndexError
instance Monad m => MultiError CacheConsistencyError  (MockDBMT m) where throw = mockThrow . IFCacheConsistencyError
instance Monad m => MultiError OffsetDecodeEx         (MockDBMT m) where throw = mockThrow . IFOffsetDecodeEx
instance Monad m => MultiError ReaderConsistencyError (MockDBMT m) where throw = mockThrow . IFReaderConsistencyError
instance Monad m => MultiError StoredOffsetsDecodeEx  (MockDBMT m) where throw = mockThrow . IFStoredOffsetsDecodeEx
instance Monad m => MultiError SegmentStreamError     (MockDBMT m) where throw = mockThrow . IFSegmentStreamError
instance Monad m => MultiError SegmentNotFoundEx      (MockDBMT m) where throw = mockThrow . IFSegmentNotFoundEx
instance Monad m => MultiError NoParse                (MockDBMT m) where throw = mockThrow . IFNoParse


data SegmentNotFoundEx = SegmentNotFoundEx Segment deriving Show

readerMain :: (MonadConc m, MonadEvaluate m, Throws '[IndexError, CacheConsistencyError, OffsetDecodeEx, ReaderConsistencyError, StoredOffsetsDecodeEx, SegmentNotFoundEx] m) => (Segment -> m (Maybe (m ByteString))) -> ReaderConfig -> ReadQueue m -> m void
readerMain openSeg ReaderConfig{..} (ReadQueue q) = go initial
    where
    initial = ReaderState (Lru.empty lruSize) (emptySegmentCache maxOpenSegs) Map.empty
    go state = do
        enqueued <- readChan q
        (state', toDispatch) <- readerStep state enqueued
        case toDispatch of
            Nothing -> return ()
            Just seg -> do
                serializer <- async $ do
                    bs <- maybe (throw (SegmentNotFoundEx seg)) id =<< openSeg seg
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

data CrashState = Interrupted | Finished

data SegmentResource m resource = SegmentResource {
    -- Does not save intermediate progress. If the program crashes, 
    -- the overwrite will be lost. Upon completion of overwrite, replaces 
    -- the Finished file and deletes any interrupted file (if present)
    overwriteSeg :: (forall a . Segment -> (resource -> m a) -> m a),
    -- If interrupted, leaves the results in a predictable place
    writeToSeg :: (forall a . Segment -> (resource -> m a) -> m a),
    loadSeg :: CrashState -> Segment -> m (Maybe (m ByteString))
}


data DBConfig m hdl = DBConfig {
        segmentResource :: SegmentResource m hdl,
        maxWriteQueueLen :: Int,
        maxReadQueueLen :: Int,
        readQueueShardShift :: Int,
        readerConfig :: ReaderConfig,
        consumerLimits :: ConsumerLimits
    }

defaultDBConfig :: FilePath -> DBConfig IO Handle
defaultDBConfig rundir = DBConfig{..}
    where
    segPath cs seg = rundir ++ "/" ++ show seg ++ (case cs of Finished -> ""; Interrupted -> "~") 
    -- filenameConfig = FilenameConfig {
    --         segmentPath = segPath Finished,
    --         partialSegmentPath = (segPath Interrupted)
    --     }
    overwriteSeg :: forall a . Segment -> (Handle -> IO a) -> IO a
    overwriteSeg segment f = do
        -- Could also do some sort of unlinking thing so the file gets deleted automatically
        let int = segPath Interrupted segment
            tmp = int ++ ".deleteme" 
        res <- withFile tmp WriteMode f
        renameFile tmp (segPath Finished segment)
        doesFileExist int >>= \case
            False -> return ()
            True -> removeFile int
        return res
    writeToSeg :: forall a . Segment -> (Handle -> IO a) -> IO a
    writeToSeg segment f = do
        res <- withFile (segPath Interrupted segment) WriteMode f
        renameFile (segPath Interrupted segment) (segPath Finished segment)
        return res
    loadSeg cs segment = do
        exists <- doesFileExist (segPath cs segment)
        return $ if exists
            then Just $ mmapFileByteString (segPath cs segment) Nothing
            else Nothing
    segmentResource = SegmentResource {..}
    maxWriteQueueLen = 16
    maxReadQueueLen = 16
    readQueueShardShift = 0
    readerConfig = ReaderConfig {
            lruSize = 16,
            maxOpenSegs = 16
        }
    consumerLimits = ConsumerLimits {
            cutoffCount = 1024,
            cutoffLength = 1024*1024
        }

-- Supports atomic read/write/rename
newtype FakeFilesystem m = FakeFilesystem (forall b . (Map FilePath Builder -> (Map FilePath Builder,b)) -> m b)

-- One ReaderT holds a reference to the mock filesystem. The other ReaderT holds a function that
-- lets us throw exceptions. DejaFu doesn't seem to support e.g. ExceptT, so instead we use
-- IO exceptions, but I want to leave "MonadIO" constrains as far outside of the mocking
-- logic as possible - the mock system should be observationally pure for the most accurate
-- and useful tests.
newtype MockDBMT m a = MockDBMT ((IrrecoverableFailure -> m Void) -- Exception throwing. Use Void because GHC doesn't like an existential here
                               -> FakeFilesystem m -- Fake filesystem
                               -> m a)
    deriving (Functor, Applicative, Monad, MonadConc, MonadThrow, MonadCatch, MonadMask, MonadIO) 
        via ReaderT (IrrecoverableFailure -> m Void) (ReaderT (FakeFilesystem m) m)

-- instance Monad m => MonadState (Map FilePath Builder) (MockDBMT m) where state = MockDBMT . lift . state

runMockDBMT :: Monad m => (forall x . IrrecoverableFailure -> m x) -> FakeFilesystem m -> MockDBMT m a -> m a
runMockDBMT mThrow mFS (MockDBMT m) = m mThrow mFS

newtype FakeHandle = FakeHandle FilePath deriving (Eq,Ord,Show)

-- MVar m (Map FilePath Builder)

mockPure :: Monad m => (Map FilePath Builder -> (Map FilePath Builder,b)) -> MockDBMT m b
mockPure f = MockDBMT $ \_ (FakeFilesystem fs) -> fs f

mockPure_ :: Monad m => (Map FilePath Builder -> Map FilePath Builder) -> MockDBMT m ()
mockPure_ f = mockPure (\x -> (f x,()))

writeToFakeHandle :: Monad m => FakeHandle -> Builder -> MockDBMT m ()
writeToFakeHandle (FakeHandle path) builder = mockPure_ $ (Map.insertWith (flip (<>)) path builder) 

deleteFakeFile :: Monad m => FilePath -> MockDBMT m ()
deleteFakeFile hdl = mockPure_ (Map.delete hdl)

renameFakeFile :: Monad m => FilePath -> FilePath -> MockDBMT m ()
renameFakeFile oldName newName = mockPure_ $ \oldMap ->
    let (oldFile, deletedFS) = alterF (\old -> (old, Nothing)) oldName oldMap in
    alter (const oldFile) newName deletedFS

doesFakeFileExist :: Monad m => FilePath -> MockDBMT m Bool
doesFakeFileExist path = mockPure $ \fs -> (fs, Map.member path fs)


mockDBConfig :: forall m . Monad m => DBConfig (MockDBMT m) FakeHandle
mockDBConfig  = DBConfig{..}
    where
    segPath cs seg = show seg ++ (case cs of Finished -> ""; Interrupted -> "~") 

    overwriteSeg :: forall a . Segment -> (FakeHandle -> MockDBMT m a) -> MockDBMT m a
    overwriteSeg segment f = do
        -- Could also do some sort of unlinking thing so the file gets deleted automatically
        let int = segPath Interrupted segment
            tmp = int ++ ".deleteme" 
        res <- f (FakeHandle tmp)
        renameFakeFile tmp (segPath Finished segment)
        doesFakeFileExist int >>= \case
            False -> return ()
            True -> deleteFakeFile int
        return res
    writeToSeg :: forall a . Segment -> (FakeHandle -> MockDBMT m a) -> MockDBMT m a
    writeToSeg segment f = do
        res <- f $ FakeHandle (segPath Interrupted segment) 
        renameFakeFile (segPath Interrupted segment) (segPath Finished segment)
        return res
    loadSeg cs segment = do -- (fmap (return . toStrict . toLazyByteString) . (Map.!? (segPath cs segment))) <$> mockPure (\fs -> (fs,fs))
        let clean = \case
                Nothing -> (Nothing,Nothing)
                Just file -> let lbs = toLazyByteString file in (Just (return $ toStrict lbs), Just (lazyByteString lbs))
            simplify = swap . alterF clean (segPath cs segment) 
        mockPure simplify
    segmentResource = SegmentResource {..}
    maxWriteQueueLen = 4
    maxReadQueueLen = 4
    readQueueShardShift = 0
    readerConfig = ReaderConfig {
            lruSize = 4,
            maxOpenSegs = 4
        }
    consumerLimits = ConsumerLimits {
            cutoffCount = 8,
            cutoffLength = 256
        }


data DBState m gc = DBState {
    dbReaders :: ReadCache m,
    dbReaderExn :: Async m Void,
    dbWriter :: WriteQueue m gc,
    dbWriterExn :: Async m Void
}

dbFinish :: MonadConc m => DBState m gc -> m Void
dbFinish DBState{..} = snd <$> waitAny [dbReaderExn, dbWriterExn]


loadInitSeg :: forall m n hdl . (Monad m, Throws '[SegmentStreamError] n, MonadConc n, Writable n) => (forall a . n a -> hdl -> m a) -> SegmentResource m hdl -> InitResult' (m ByteString) -> m Segment
loadInitSeg run SegmentResource{..} status = case status of
        NoData' -> return (Segment 0)
        CleanShutdown' highestSeg _loadHighestSeg -> return (succ highestSeg)
        AbruptShutdown' partialSeg  loadPartialSeg -> do
            partial <- loadPartialSeg
            overwriteSeg partialSeg $ \hdl -> do
                let entries = streamBS partial
                    fakeWrite bs = do
                        ref <- newEmptyMVar
                        flushed <- newEmptyMVar
                        return $ Write ref flushed bs ()
                    fakeWriteQ = SP.mapM fakeWrite entries
                    config :: ConsumerConfig n
                    config = ConsumerConfig partialSeg (const (return ()))
                _arbitraryGC :> _savedOffs <- run (consumeFile @n config NoGC fakeWriteQ) hdl
                return (succ partialSeg)

-- consumeFile :: forall m r . (MonadConc m, Writable m) => ConsumerConfig m -> Maybe Ref -> Map SeshID (Set Ref) -> Stream (Of (WEnqueued m)) m r -> m (Of (Maybe Ref, Map SeshID (Set Ref)) r)

-- Scans backwards through the latest segments, to set up the GC as needed.
initializeGC :: forall m gc . (Monad m, Throws '[SegmentNotFoundEx, OffsetDecodeEx, StoredOffsetsDecodeEx, NoParse] m, GCModel gc) => (Segment -> m (Maybe (m ByteString))) -> Segment -> m gc
initializeGC load seg = f (initializer @gc)
        where
        f (Initializer s0 step finish) = go s0 seg
            where
            go gcInit strictlyTooHigh 
                | strictlyTooHigh == minBound = either throw return $ finish gcInit
                | otherwise = do
                    let viable = pred strictlyTooHigh
                    raw <- load viable >>=  maybe (throw (SegmentNotFoundEx viable)) id 
                    offs <- either throw return $ loadOffsets raw
                    let mmapped = MMapped offs raw
                        findWithin state stream = next stream  >>= \case
                            Left () -> go state (pred strictlyTooHigh)
                            Right ((ix,bs),stream') -> case step state (Ref viable ix) bs of
                                Left (Left noParse) -> throw noParse
                                Left (Right done) -> return done
                                Right continue -> findWithin continue stream'
                    findWithin gcInit (streamSegFromOffsets Desc mmapped) 



-- NB: Setup is completely pure. An instantiation of this function need not use IO.
-- I was hoping to be able to write setupMock without even using MonadIO, but due
-- to the structure of dejafu (esp. wrt. exceptions) I was unable to.
setup :: forall m n hdl gc . (MVar m ~ MVar n, MonadConc m, MonadConc n, MonadEvaluate m, 
          Throws '[IndexError, CacheConsistencyError, OffsetDecodeEx, ReaderConsistencyError, StoredOffsetsDecodeEx, SegmentNotFoundEx, NoParse] m, 
          Throws '[SegmentStreamError] n, Writable n,
          GCModel gc) 
      => (forall a . n a -> hdl -> m a) -> (forall a . m a -> n a)  -> DBConfig m hdl -> m (DBState m gc)
setup run hLift DBConfig{..} = do
    status <- initialize' (loadSeg segmentResource)
    initSeg <- loadInitSeg run segmentResource status
    initGC <- initializeGC (loadSeg segmentResource Finished) initSeg :: m gc
    (readers, readersExn) <- spawnReaders maxReadQueueLen readQueueShardShift (loadSeg segmentResource Finished) readerConfig
    hotCache <- newMVar (initSeg, mempty)
    (writer, writerExn) <- spawnWriter run hLift hotCache maxWriteQueueLen initSeg initGC segmentResource consumerLimits
    return $ DBState (ReadCache readers hotCache) readersExn writer writerExn

setupIO :: forall gc .  GCModel gc => DBConfig IO Handle -> IO (DBState IO gc)
setupIO = setup run (lift . lift)
    where run write = runHandleT (runBufferT write)


instance Monad m => MonadEvaluate (MockDBMT m) where
    evaluateM = return

-- Introduce MonadIO here only for exception catching that supports DejaFu
-- (and MonadEvaluate, although we could just put evaluateM = return if we wanted)
setupMock :: forall gc m . (MonadConc m, GCModel gc) => DBConfig (MockDBMT m) FakeHandle -> MockDBMT m (DBState (MockDBMT m) gc)
setupMock = setup run lift 
    where 
    run write hdl = runDummyFileT write (writeToFakeHandle hdl) 


data InitResult' k  = NoData' | CleanShutdown' Segment k | AbruptShutdown' Segment k 

-- TODO: Not initializing if there's stuff already there
initialize' :: Monad m => (CrashState -> Segment -> m (Maybe k)) -> m (InitResult' k)
initialize' find = do
    find Finished 0 >>= \case
        Nothing -> find Interrupted 0 >>= \case
            Nothing -> return NoData'
            Just k -> return $ AbruptShutdown' 0  k
        Just k -> do
            (expMin,expLoad) <- exponentialBounds (find Finished) (0,k)
            (binExact,binLoad) <- binarySearch (find Finished) (expMin,expLoad) (expMin * 2)
            find Interrupted (binExact + 1) >>= \case
                Nothing -> return $ CleanShutdown' binExact binLoad
                Just k' -> return $ AbruptShutdown' (binExact + 1) k'

exponentialBounds :: (Num key, Monad m) => (key -> m (Maybe val)) -> (key,val) -> m (key, val)
exponentialBounds find zero = go 1 zero
    where 
    go n highest = do
        val <- find n
        maybe (return highest) (go (n*2)) ((n,) <$> val)        
-- invariant: There are certainly no keys higher than `hi` which might return `Just _`
-- lo is guaranteed to return `Just _`
-- find is monotonic
binarySearch :: (Integral key, Eq key, Monad m) => (key -> m (Maybe val)) -> (key,val) -> key -> m (key,val)
binarySearch f = \(lo,lv) hi -> go lo lv hi 
    where
    go lo lv hi | lo == hi = return (lo,lv)
                | otherwise = f testKey >>= maybe (go lo lv (testKey - 1)) (\tv -> go testKey tv hi)
                    where
                    testKey = (lo + hi + 1) `div` 2




-- -- Sharded by segment hash
data Readers m = Readers {shardShift :: Int, 
                          shards :: V.Vector (ReadQueue m)}

readViaReaders :: MonadConc m => Readers m -> Ref -> m (MVar m ByteString)
readViaReaders Readers{..} ref = do
    let segHash = hash (refSegment ref)
        mask = (1 `shiftL` shardShift) - 1 
        hash' = segHash .&. mask
        ReadQueue rq = shards V.! hash'
    readVar <- newEmptyMVar
    writeChan rq $ Read ref readVar
    return readVar


spawnWriter :: forall m n hdl gc . (MonadConc m, MVar m ~ MVar n, MonadConc n, Writable n, GCModel gc) => (forall a . n a -> hdl -> m a) -> (forall a . m a -> n a) -> MVar m (Segment, Map Offset ByteString) -> Int -> Segment -> gc -> SegmentResource m hdl -> ConsumerLimits -> m (WriteQueue m gc, Async m Void )
spawnWriter run liftN hotCache maxWriteQueueLen initSeg initGc SegmentResource{..} consumerLimits = do
    writeQ <- newWriteQueue maxWriteQueueLen
    let chunks = hoist lift $ splitWith @m consumerLimits (streamWriteQ writeQ)
        writeSeg :: Segment -> gc -> Stream (Of (WEnqueued (MVar m) gc)) m x -> m (gc, x)
        writeSeg seg gc writes = do
            let registerHotcache :: Map Offset ByteString -> m ()
                registerHotcache offsets = swapMVar @m hotCache (seg, offsets) >> return ()
                consumeToHdl = consumeFile @n (ConsumerConfig seg (liftN . registerHotcache)) gc (hoist liftN writes)
            gc' :> r <- writeToSeg seg (run consumeToHdl)
            return (gc', r)
        writeSegS :: Stream (Of (WEnqueued (MVar m) gc)) m x -> StateT (Segment, gc) m x
        writeSegS writes = do
            (seg, gc) <- get
            (gc', x) <- lift $ writeSeg seg gc writes
            put (succ seg, gc')
            return x
    exn <- async $ flip evalStateT (initSeg, initGc) $ mapsM_ writeSegS chunks
    return (writeQ, exn)

spawnReaders :: (MonadConc m, MonadEvaluate m, Throws '[IndexError, CacheConsistencyError, OffsetDecodeEx, ReaderConsistencyError, StoredOffsetsDecodeEx, SegmentNotFoundEx] m) 
             => Int -> Int -> (Segment -> m (Maybe (m ByteString))) -> ReaderConfig -> m (Readers m, Async m void)
spawnReaders qLen shardShift openSeg cfg = do
    let spawn = do
            readQ <- readQueue qLen
            thread <- async $ readerMain openSeg cfg readQ
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

-- How do we track GC snapshots?
-- What behavior do we need to guarantee to ensure that there are no reads on GC'd values?
-- Roots refers to all refs that are extant in RAM (ish)
-- We should use the ST variable-scope-escape trick
-- 1. Reads
-- When a session is opened on a root, we add the root to the live set. (or map - we sort of want to RC active roots)
-- All reads not created in this session must descend from that root.
-- Actually, instead of having a separate live set we can just put it in the `Map SeshID (Set Ref)` I suppose. Or can we?
-- We'll have to pass the Map SeshID (Set Ref) across multiple Writers, I suppose.
-- 2. Writes
-- The Writer process keeps a set of all newly created values. Or maybe `Map SeshID (Set Ref)`?
-- Only one session is entitled to access newly created values (enforced w/ST trick), 
-- until the session commits (values are tacked onto roots). Figure out an API for that.
-- When session finishes, it fills an MVar indicating that it has done so.
-- When the writer flushes a log segment, it figures out which sessions have finished. Dumps those
-- from the map. Remainder are still live.
-- How do we persist writes from a session? Maybe it can return a new root ref?

-- class ReadSesh ref m where
--     read :: ref (a ref) -> m (a ref)

--     -- write :: a (ref b)

-- newtype ReadSeshM m a = 
--     ReadSeshM (
        
--     )
-- data Root

-- runReadSession :: (forall ref m . ReadSesh ref m => ref Root -> m a) -> ReadSeshM m a
-- runReadSession x = undefined

-- Whenever the writer finishes a segment, it takes a snapshot of all active roots (which can only shrink
-- wrt that segment) and passes it to the GC
data GcSnapshot = GcSnapshot {snapshotSegment :: Segment, snapshotRoots :: Set Ref}


spawnGcManager :: (Monad m, MonadConc m, Throws '[StoredOffsetsDecodeEx, GCError] m, 
                   Writable n, Throws '[OffsetDecodeEx, GCError] n,
                   RandomGen g)
               => (forall a . n a -> hdl -> m a) -> (ByteString -> Either NoParse (VS.Vector Ref)) 
               -> SegmentResource m hdl -> Chan m GcSnapshot -> (Segment -> MMapped -> m ()) 
               -> g
               -> m void
spawnGcManager run childrenOf SegmentResource{..} completeSegs registerGC = \gen -> forever (readChan completeSegs >>= peristaltize gen)
    where
    peristaltize gen GcSnapshot{..} | not (null newerRefs) = error "Roots persist from older segments"
                                -- | null currentIxes =  -- optimization: Just save an empty segment
                                | otherwise = do
                                    let load = loadSeg Finished snapshotSegment >>= maybe (throw (GCSegmentLoadError snapshotSegment)) id 
                                    old_bs <- load
                                    old_offs <- either throw return $ loadOffsets old_bs
                                    let old = MMapped old_offs old_bs
                                        collect = gcOneSegment childrenOf snapshotSegment currentIxes old
                                    (new_offs, activeRefs) <- overwriteSeg snapshotSegment (run collect)
                                    new_bs <- load
                                    registerGC snapshotSegment (MMapped new_offs new_bs)
                                    let olderRefs' = union olderRefs activeRefs
                                        (score, gen') = randomR (0.0, 1.0 :: Double) gen
                                    if score < 0.1 || snapshotSegment == minBound
                                        then return ()
                                        else peristaltize gen' (GcSnapshot {snapshotRoots = olderRefs', snapshotSegment = pred snapshotSegment})

        where
        (olderRefs, currentRefs, newerRefs) = let (lo,hi) = segBound snapshotSegment in setRange lo hi snapshotRoots
        currentIxes = Set.map refIx currentRefs


data ReadCache m = ReadCache {
        readers :: Readers m,
        -- Everyone has to read this for every read. Seems bad. Weak IORef MM causes issues, must use something with strong MM (like MVar)
        hot :: MVar m (Segment, Map Offset ByteString)
    }


readViaReadCache :: MonadConc m => ReadCache m -> Ref -> m (Async m ByteString)
readViaReadCache ReadCache{..} ref  = async $ do
    (hotSegment, hotcache) <- readMVar hot
    if hotSegment == refSegment ref
        then return $ snd (Map.elemAt (fromEnum $ refIx ref) hotcache)
        else do
            readVar <- readViaReaders readers ref 
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

splitWith :: forall m a gc . Monad m
          => ConsumerLimits 
          -> Stream (Of (WEnqueued (MVar m) gc)) m a 
          -> Stream (Stream (Of (WEnqueued (MVar m) gc)) m) m a
splitWith limits = splitAfter throughput (0,0) (`exceeds` limits) 
    where
    throughput (count,len) (Write _ _ bs _) = (count + 1, len `plus` ByteString.length bs)
    throughput tp          _                  = tp




data Persistence = NormalGC
                 | PersistentRoot deriving (Show)

data NoParse = NoParse String deriving Show




data SegGcState = SegGcState {
    liveHere :: Set Ix,
    liveThere :: Set Ref
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

newtype DummyFileT m a = DummyFileT (ReaderT (Builder -> m ()) m a)
    deriving newtype (Functor, Applicative, Monad, MonadIO, MonadConc, MonadThrow, MonadCatch, MonadMask)

instance MonadTrans DummyFileT where
    lift = DummyFileT . lift


instance Monad m => Writable (DummyFileT m) where
    {-# INLINE writeM #-}
    writeM bs = writeMB $ byteString bs
    {-# INLINE writeMB #-}
    writeMB bd = DummyFileT $ ask >>= \f -> lift (f bd)
    flushM = return ()

runDummyFileT :: DummyFileT m a -> (Builder -> m ()) -> m a
runDummyFileT (DummyFileT reader) = runReaderT reader

instance MultiError e m => MultiError e (DummyFileT m) where
    throw = lift . throw

data GCError
    = GCDeserializeError 
    | SegmentCausalityViolation Segment Ref 
    | IndexCausalityViolation Ix Ref 
    | GCParseError NoParse
    | GCSegmentLoadError Segment
    deriving Show

gcOneSegment :: forall m . (Throws '[GCError, OffsetDecodeEx] m, Writable m) => (ByteString -> Either NoParse (VS.Vector Ref)) -> Segment -> Set Ix -> MMapped -> m (StoredOffsets, Set Ref)
gcOneSegment childrenOf thisSegment live = fmap unwrap . repackFile . gc' . streamSegFromOffsets Desc
    where 
    unwrap (offs :> live' :> ()) = (offs, live')
    gc' :: forall o . Throws '[GCError] m => Stream (Of (Ix, ByteString)) m o -> Stream (Of (Ix, ByteString)) m (Of (Set Ref) o)
    gc' = foldM include (return (SegGcState live Set.empty)) (return . liveThere) . hoist lift
        where
        include state (ix,bs) = do
            children <- either (lift . throw . GCParseError) return $ childrenOf bs
            if ix `Set.member` (liveHere state) 
                then do
                    yield (ix,bs)
                    includeChildren state ix children
                else return state
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
        writeM $ encode $ toEnum @Word64 $ ByteString.length bs
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


newtype SeshID = SeshID Word64
    deriving (Eq,Ord,Enum)


data ConsumerState gc m = ConsumerState {
        gcState :: gc,
        entries :: Map Offset ByteString,
        writeOffset :: Offset,
        flushQueue :: [MVar m Flushed]
    }

data Initializer result where
    Initializer :: x -> (x -> Ref -> ByteString -> Either r x) -> (x -> r) -> Initializer r
    {-             ^    ^                                    ^    ^
                   |    '------------------------------------'    '-nothing left, finish up
                   |             check if this is a root
                   initial state
   -}

class GCModel gc where
    type Initial gc :: * -- The information that a session receives from the GC engine when it starts
    type Final gc :: * -- The information that a session passes to the GC engine to close the session
    -- type Snapshot gc :: *  -- The information that the consumer passes to the collector when it's time to GC
    type WriteData gc :: * -- Metadata accompanying a write request
    initSession :: gc -> (Initial gc, gc)
    endSession :: gc -> Final gc -> gc
    registerWrite :: gc -> Ref -> WriteData gc -> gc
    initializer :: Initializer (Either NoParse gc)
    -- snapshot :: proxy gc -> Serial gc -> Snapshot gc

data PlainGC semantics = PlainGC {
        latestRoot :: Maybe Ref,
        sessions :: Map SeshID (Set Ref)
    }

instance ByteSemantics semantics => GCModel (PlainGC semantics) where
    type Initial   (PlainGC semantics) = (SeshID, (Maybe Ref))
    type Final     (PlainGC semantics) = SeshID
    type WriteData (PlainGC semantics) = (SeshID, IsRoot)
    initSession PlainGC{..} = ((newSeshID, latestRoot),PlainGC latestRoot newSessions)
        where
        newSeshID = case Map.lookupMax sessions of
            Nothing -> SeshID 0
            Just (seshID,_) -> succ seshID
        newSessions = Map.insert newSeshID (maybe Set.empty Set.singleton latestRoot) sessions
    endSession PlainGC{..} seshID = PlainGC latestRoot (Map.delete seshID sessions)
    registerWrite PlainGC{..} ref (seshID, isRoot) = PlainGC latestRoot' sessions'
        where
        latestRoot' = case isRoot of YesRoot -> Just ref; NotRoot -> latestRoot
        sessions' = Map.adjust (Set.insert ref) seshID sessions
    initializer = Initializer () (\() ref bs -> case semanticIsRoot (Proxy @semantics) bs of
                                                    Left noParse -> Left $ Left noParse
                                                    Right YesRoot -> Left $ Right $ PlainGC (Just ref) Map.empty
                                                    Right NotRoot -> Right ())
                                 (\() -> Right $ PlainGC Nothing Map.empty)

data NoGC = NoGC

instance GCModel NoGC where
    type Initial NoGC = ()
    type Final NoGC = ()
    type WriteData NoGC = ()
    initSession NoGC = ((), NoGC)
    endSession NoGC () = NoGC
    registerWrite NoGC _ref () = NoGC
    initializer = Initializer () (\() _ -> const (Left $ Right NoGC)) (const $ Right NoGC)


data SegmentStep = Incomplete | Finalizer ByteString | Packet ByteString ByteString


class ByteSemantics sem where
    semanticIsRoot :: proxy sem -> ByteString -> Either NoParse IsRoot
    semanticChildren :: proxy sem -> ByteString -> Either NoParse (VS.Vector Ref)



-- instance ByteSemantics Root where


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

data ConsumerAction m gc = Flush [MVar m Flushed] | WriteToLog ByteString (Map Offset ByteString) (MVar m Ref) Ref | Nop | InitSesh (MVar m (Initial gc)) (Initial gc)


consume :: (Monad m, GCModel gc) 
        => (ConsumerAction m gc -> m ()) 
        -> (ConsumerState gc m -> m o)
        -> Segment
        -> gc
        -> Stream (Of (WEnqueued (MVar m) gc)) m r -> m (Of o r)
consume handle finalize thisSeg gc = foldM step initial finalize
    where
    initial = return (ConsumerState gc Map.empty (Offset 0) [])
    step state write = do
        let (state', action) = consume' thisSeg state write
        handle action
        return state'


consume' :: GCModel gc => Segment -> ConsumerState gc m -> WEnqueued (MVar m) gc -> (ConsumerState gc m, ConsumerAction m gc)
consume' thisSeg state@ConsumerState{..} = \case
    Tick -> if null flushQueue
        then (state, Nop)
        else (state {flushQueue = []}, Flush flushQueue)
    Write refVar flushed bs gcData -> do
        let entries' = Map.insert writeOffset bs entries
            writeOffset' = writeOffset `plus` (ByteString.length bs + 8)
            ix = Ix (fromIntegral $ length entries)
            ref = Ref thisSeg ix
            gc' = registerWrite gcState ref gcData
        (ConsumerState {entries = entries', writeOffset = writeOffset', flushQueue = flushed : flushQueue, gcState = gc'},
            WriteToLog bs entries' refVar ref)
    SeshInit seshRef -> do
        let (initial, gc') = initSession gcState
        (state {gcState = gc'}, InitSesh seshRef initial)
    SeshDone seshID -> (state {gcState = endSession gcState seshID}, Nop)


{-# INLINE consumeFile #-}
consumeFile :: forall m r gc . (MonadConc m, Writable m, GCModel gc) => ConsumerConfig m -> gc -> Stream (Of (WEnqueued (MVar m) gc)) m r -> m (Of gc r)
consumeFile cfg = consume (saveFile (register cfg)) (\state -> fmap (\() -> gcState state) (finalizeFile cfg state)) (segment cfg)


{-# INLINE saveFile #-}
saveFile :: (MonadConc m, Writable m) => (Map Offset ByteString -> m ()) -> ConsumerAction m gc -> m ()
saveFile registerHotcache action = case action of
        Flush flushed -> do
            flushM
            mapM_ (`putMVar` Flushed) flushed
        WriteToLog bs entries refVar ref -> do
            writeM $ encode $ toEnum @Word64 $ ByteString.length bs
            writeM bs
            -- This is a bit confusing: register is supposed to update the shared "hot" cache for the active write head.
            -- We *must* make a globally visible update to the hot cache before returning the ref, or else
            -- someone could try to do a read that would fail.
            -- Can't use IORef - weak memory model. MVar should be fine. STM/MVar interaction OK?
            registerHotcache entries
            putMVar refVar ref
        Nop -> return ()
        InitSesh ref sesh -> putMVar ref sesh 

writeOffsetsTo :: Writable m => StoredOffsets -> m ()
writeOffsetsTo offsets = do
    let storedOffsets = encode offsets  
        offsetsLen = fromIntegral (ByteString.length storedOffsets) :: Word64 
    writeM $ encode (maxBound :: Word64)
    writeM $ encode offsetsLen
    writeM storedOffsets -- length on either side so we can read from the end of file or in order
    writeM $ encode offsetsLen
    flushM

finalizeFile :: (MonadConc m, Writable m) => ConsumerConfig m -> ConsumerState gc m -> m ()
finalizeFile ConsumerConfig{..} ConsumerState{..} = do
    flushM
    mapM_ (`putMVar` Flushed) flushQueue
    let offsets = OffsetVector (VS.fromList (keys entries))
    writeOffsetsTo offsets


demoIO :: IO () 
demoIO = do
    let cfg = defaultDBConfig "/tmp/rundir"
    putStrLn "Setting up"
    state <- setupIO @NoGC cfg 
    putStrLn "Writing"
    let wq = dbWriter state
    writes <- replicateM (8*1024) $ storeToQueue wq (ByteString.replicate (16 {- *4096 -}) 0x44) ()
    putStrLn "Waiting"
    refAsyncs <- async $ do
        (refs,flushes) <- unzip <$> mapM wait writes
        mapM_ takeMVar flushes
        return refs
    putStrLn "Flushing"
    flushWriteQueue wq
    putStrLn "Waiting for flush"
    (_, refs) <- waitAny [absurd <$> dbReaderExn state, absurd <$> dbWriterExn state, refAsyncs]
    print (length refs)


-- TODO: When there's no rundir, we get a dumb STM errors. Probably not dealing with exceptions correctly
-- TODO: Also, can this lock up? Should run this under test
demoIO2 :: IO () 
demoIO2 = do
    let cfg = defaultDBConfig "/tmp/rundir"
    putStrLn "Setting up"
    state <- setupIO @NoGC cfg 
    ref <- async $ do
        let action :: DBT NoGC IO Int = do
                gist :: G.GiST Reft VU.Vector (G.Within Int) Int Int <- Gex.bigSet' (G.FillFactor 8 16) 100000
                G.foldl' G.read (+) 0 gist
        total <- runDBT action state
        print total
        -- Now flush
        let wq = dbWriter state 
        write <- storeToQueue wq "All done" ()
        (ref, flush) <- wait write
        flushWriteQueue wq
        Flushed <- takeMVar flush
        return ref
    (_,_) <- waitAny [absurd <$> dbReaderExn state, absurd <$> dbWriterExn state, ref]
    return ()
    -- flushWriteQueue $ dbWriter state
    -- putStrLn "Waiting for flush"
    -- (_, refs) <- waitAny [absurd <$> dbReaderExn state, absurd <$> dbWriterExn state, refAsyncs]
    -- print (length refs)

newtype DBT gc m a = DBT {runDBT :: DBState m gc -> m a}
    deriving (Functor, Applicative, Monad, MonadReader (DBState m gc)) via (ReaderT (DBState m gc) m)
instance MonadTrans (DBT gc) where lift = DBT . const

type role Reft nominal
data Reft a where
    Reft :: Store a => Ref -> Reft a

instance Store a => Store (Reft a) where
    size = contramap (\(Reft r) -> r) Store.size
    peek = fmap Reft Store.peek
    poke = Store.poke . (\(Reft r) -> r)


-- NB: This does not deal with transactions or flushing
instance (MonadConc m, MonadEvaluate m)=> G.BackingStore (DBT NoGC m) Reft Reft VU.Vector (G.Within Int) Int Int where
    saveS vector = do
        wq <- dbWriter <$> ask
        lift $ do
            pending <- storeToQueue wq (encode vector) ()
            (ref, _flushed) <- wait pending
            return (Reft ref)
    leaveS _ = id

instance MonadConc m => G.R (DBT NoGC m) Reft where
    -- type Referent (DBT NoGC m) a = Store a
    read (Reft ref) = ask >>= \state -> lift $ do
        let rc = dbReaders state
        val_ <- readViaReadCache rc ref
        val <- wait val_
        return $ decodeEx val


{-# SPECIALIZE test :: Map FilePath Builder -> DBConfig (MockDBMT IO) FakeHandle -> (forall n . (MonadConc n, MonadEvaluate n) => DBT NoGC n a) -> IO (Map FilePath Builder, a) #-}
test :: MonadConc m => Map FilePath Builder -> DBConfig (MockDBMT m) FakeHandle -> (forall n . (MonadConc n, MonadEvaluate n) => DBT NoGC n a) -> m (Map FilePath Builder, a)
test initialFS cfg theTest =  do
    fsState <- newMVar initialFS
    let fs = FakeFilesystem $ \f -> modifyMVar fsState (return . f)
    result <- runMockDBMT throwM fs $ do
        state <- setupMock @NoGC cfg 
        runDBT theTest state
    finalFS <- readMVar fsState
    return (finalFS, result)


