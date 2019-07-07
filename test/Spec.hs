import Lib.System 
import Test.QuickCheck (quickCheck, ioProperty)
import System.Random 
import           System.Random (RandomGen, randomR, random, newStdGen, randomRs)
import qualified Crypto.Random as Random
import           Control.Concurrent.Classy.Async (Async, async, link, waitAny, wait)
import           Data.Void (Void, absurd)
import           Data.Proxy (Proxy(..))
import           Data.Functor.Identity (Identity, runIdentity)
import           Control.Monad (foldM)
import           Control.Concurrent.Classy.MVar (MVar, newEmptyMVar, takeMVar, putMVar, swapMVar, readMVar, newMVar, modifyMVar)

import           Control.Concurrent.Classy (MonadConc)
import           Data.Map.Strict as Map (Map, (!?), member)
import qualified Data.Map.Strict as Map
import           Data.ByteString.Builder (Builder, byteString, hPutBuilder, toLazyByteString)
import           Data.ByteString (ByteString)
import qualified Data.ByteString as ByteString
import           Data.ByteString.Lazy (toStrict)

import           Test.Tasty.DejaFu (testDejafus)
import           Test.Tasty.QuickCheck (testProperty)
import           Test.Tasty (defaultMain, testGroup)
import           Test.DejaFu (deadlocksNever, exceptionsNever)

import           Test.QuickCheck as Q

import           Data.List (sortOn)

import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Storable as VS
import qualified Data.Vector.Generic as VG

import           Data.Word (Word8, Word64)

import qualified Lib.GiST as G
import qualified Lib.GiST_ex as Gex

import qualified Criterion.Main as CM


main :: IO ()
main = do
    tasty
    criterion

tasty :: IO ()
tasty = defaultMain $ testGroup "All tests" $ 
    [ testProperty "GiST over Identity has equivalent behavior to a map (U/I/C)" (testMapEquivalence @VU.Vector @Int @Char Proxy)
    , testProperty "GiST over Identity has equivalent behavior to a map (U/8/I)" (testMapEquivalence @VU.Vector @Word8 @Int Proxy)
    , testProperty "GiST over Identity has equivalent behavior to a map (U/I/I)" (testMapEquivalence @VU.Vector @Int @Int Proxy)
    , testProperty "GiST over Identity has equivalent behavior to a map (V/64/S)" (testMapEquivalence @V.Vector @Word64 @String Proxy)
    , testProperty "Writing then reading works as expected" (ioProperty . readsFollowWrites)
    , testDejafus [("No deadlocks", deadlocksNever), ("No exceptions", exceptionsNever)] demoMock
    ]

criterion :: IO ()
criterion = return ()
    -- CM.defaultMain [
    --     CM.bgroup "
    -- ]

-- Can be used in IO (for unit test) or with Dejafu (deadlock-free verification)
demoMock :: MonadConc m => m (Map FilePath ByteString)
demoMock = fmap (fmap (toStrict . toLazyByteString) . fst) $ test Map.empty mockDBConfig $ \state -> do
    let wq = dbWriter state
    let rc = dbReaders state
    writes <- mapM (\c -> storeToQueue wq (ByteString.replicate (20) c) ()) $ take 2 [0x44..]
    refsAsyncs <- async $ unzip <$> mapM wait writes
    (_, (refs, flushes)) <- waitAny [absurd <$> dbReaderExn state, absurd <$> dbWriterExn state, refsAsyncs]
    flushWriteQueue wq
    mapM_ takeMVar flushes
    readAll <- mapM (`readViaReadCache` rc) refs 
    _ <- waitAny readAll
    return ()

readsFollowWrites :: Int -> IO Bool
readsFollowWrites seed = do
    let (s1,s2) = split (mkStdGen seed)
    fs <- genRandomFilesystem mockDBConfig s1
    (_fs', ok) <- test fs mockDBConfig (readEqualsWrite s2)
    return ok

genRandomFilesystem :: forall m g . (MonadConc m, RandomGen g) => DBConfig (MockDBMT m) FakeHandle -> g -> m (Map FilePath Builder)
genRandomFilesystem = \cfg g ->
    fmap fst $ test Map.empty cfg $ \state -> do
        let wq = dbWriter state
        writes <- mapM (\bs -> storeToQueue wq bs ()) $ randomBSs g
        (_refs, flushes) <- unzip <$> mapM wait writes
        flushWriteQueue wq
        mapM_ takeMVar flushes


randomBSs :: RandomGen g => g -> [ByteString]
randomBSs g = fst $ Random.withDRG chacha $ mapM Random.getRandomBytes lens
    where
    (len, gSeed) = randomR (0,0xFF) g 
    (seed, gLens) = random gSeed
    lens = take len $ randomRs (0,0xFF) gLens
    chacha = Random.drgNewTest (seed, 0, 0, 0, 0)


readEqualsWrite :: forall m g . (MonadConc m, MonadEvaluate m, RandomGen g) => g -> DBState m NoGC -> m Bool
readEqualsWrite g state = do
    let wq = dbWriter state
    let rc = dbReaders state
    let byteStrings = randomBSs g
    writes <- mapM (\bs -> storeToQueue wq bs ()) byteStrings
    refsAsyncs <- async $ unzip <$> mapM wait writes
    (_, (refs, flushes)) <- waitAny [absurd <$> dbReaderExn state, absurd <$> dbWriterExn state, refsAsyncs]
    readAll1 <- mapM (`readViaReadCache` rc) refs 
    readByteStrings1 <- mapM wait readAll1
    flushWriteQueue wq
    mapM_ takeMVar flushes
    readAll2 <- mapM (`readViaReadCache` rc) refs 
    readByteStrings2 <- mapM wait readAll2
    return (byteStrings == readByteStrings1 && readByteStrings1 == readByteStrings2)

instance Q.Arbitrary G.FillFactor where
    arbitrary = do
        min <- choose (1,20)
        max <- (min +) <$> choose (1,20)
        return (G.FillFactor min max)

testMapEquivalence :: forall vec key val proxy . (G.IsGiST vec (G.Within key) key val, Eq val, Eq (vec (key,val))) => proxy vec -> G.FillFactor -> [(key,val)] -> Bool
testMapEquivalence _ fill assocs = runIdentity $ go (Proxy :: Proxy (G.GiST Identity vec (G.Within key) key val)) fill assocs
    where
    go :: forall vec set k v m rw proxy . (G.IsGiST vec set k v, G.BackingStore m rw rw vec set k v, Monad m, G.R m rw, Eq (vec (k,v)), Ord k, Eq v) => proxy (G.GiST rw vec set k v) -> G.FillFactor -> [(k,v)]-> m Bool
    go _ ff assocs = do
            theGiST <- create 
            (&&) <$> accessEquivalence theGiST <*> foldEquivalence theGiST
        where
        theMap = Map.fromList assocs
        elems = Map.toList theMap -- deduplicated
        create = G.empty >>= \empty -> foldM (\g (k,v) -> G.insert @vec @set ff k v g) empty  elems
        accessEquivalence g = and <$> mapM (testAccess g) elems
        testAccess g (k,v) = do
            matching <- G.list (G.exactly k) g
            return $ matching == VG.singleton (k,v)
        foldEquivalence gist = do
            gistList <- G.foldr G.read (:) [] gist
            gistListK <- G.foldri G.read (:) [] gist
            return (gistListK == Map.toList theMap && gistList == Map.elems theMap)

