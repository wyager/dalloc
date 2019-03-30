import Lib.System 
import Test.QuickCheck (quickCheck, ioProperty)
import System.Random 
import           System.Random (RandomGen, randomR, random, newStdGen, randomRs)
import qualified Crypto.Random as Random
import           Control.Concurrent.Classy.Async (Async, async, link, waitAny, wait)
import           Data.Void (Void, absurd)
import           Control.Concurrent.Classy.MVar (MVar, newEmptyMVar, takeMVar, putMVar, swapMVar, readMVar, newMVar, modifyMVar)

import           Control.Concurrent.Classy (MonadConc)
import           Data.Map.Strict as Map (Map, (!?), member, insert, insertWith, alterF, keys, empty, alter, updateLookupWithKey, adjust, delete, lookup, singleton, traverseWithKey, elemAt)
import           Data.ByteString.Builder (Builder, byteString, hPutBuilder, toLazyByteString)
import           Data.ByteString (ByteString)
import qualified Data.ByteString as ByteString
import           Data.ByteString.Lazy (toStrict)

import           Test.Tasty.DejaFu (testDejafus)
import           Test.Tasty.QuickCheck (testProperty)
import           Test.Tasty (defaultMain, testGroup)
import           Test.DejaFu (deadlocksNever, exceptionsNever)


main :: IO ()
main = defaultMain $ testGroup "All tests" $ 
    [ testProperty "Writing then reading works as expected" (ioProperty . readsFollowWrites)
    , testDejafus [("No deadlocks", deadlocksNever), ("No exceptions", exceptionsNever)] demoMock
    ]

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
