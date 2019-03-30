module Main (main) where

import Lib.System (demoIO)

-- import Test.DejaFu (autocheck)

-- import Control.Monad (replicateM)

main :: IO ()
main = demoIO --  autocheck demoMock >>= print

-- import           Control.Concurrent.MVar (MVar, newEmptyMVar, takeMVar, putMVar)
-- import           Control.Concurrent.STM.TMVar (TMVar, newEmptyTMVarIO, takeTMVar, putTMVar)
-- import           Control.Concurrent.STM (atomically)
-- import Control.Concurrent (forkIO)



-- import Lib
-- import Lib.Writer as W
-- import Lib.StoreStream as S
-- import qualified Data.Store as Store
-- import qualified Data.ByteString as ByteString
-- import qualified Data.ByteString.Lazy as ByteStringL
-- import qualified Data.Vector.Unboxed as VecU
-- import qualified Data.Vector.Storable as VecS
-- import qualified Data.Vector as Vec
-- import qualified Data.Vector.Generic as VecG
-- import qualified Data.Binary as Bin
-- import qualified Data.Binary.Put as BinP
-- import qualified Data.Serialize as Ser
-- import qualified Data.Serialize.Put as SerP
-- import           Data.Complex (Complex((:+)))
-- import           GHC.Generics (Generic)
-- import qualified Data.Map.Strict as Map
-- import           System.IO (withFile, IOMode(WriteMode))
-- import qualified Streaming.Prelude as Stream
-- import qualified Data.ByteString as ByteString
-- import qualified Control.Concurrent.MVar as MVar
-- import           Data.Word (Word8)

-- main = do
--     result <- withFile "/dev/stdout" WriteMode $ \hdl -> 
--         W.save hdl $ 
--             Stream.map (fmap S.store) $ 
--             Stream.replicateM 10000000 $ do
--                 return (W.Writeback $ Write (const (return ())) (ByteString.replicate 10000 (0x44 :: Word8)))
--     print result


-- data Crack a = Crack !a !Int (VecU.Vector (Int,Int)) deriving Generic
-- instance Store.Store a => Store.Store (Crack a)

-- data Tree = Tree Int Tree Tree | Empty deriving Generic
-- instance Store.Store Tree


-- data Btree (m :: * -> * -> *) k a = Btree a (m k a) 
--     deriving stock Generic

-- instance (Store.Store a, Store.Store (m k a)) => Store.Store (Btree m k a)


-- newtype UVMap k a = UVMap (VecU.Vector (k,a)) 
--     deriving stock Generic

-- instance (Store.Store (VecU.Vector a), Store.Store (VecU.Vector b)) => Store.Store (UVMap a b)


-- newtype BUV = BUV {buv :: Btree UVMap Int Int} deriving Generic
-- instance Store.Store BUV
 
-- newtype BMV = BMV {bmv :: Btree Map.Map Int Int} deriving Generic
-- instance Store.Store BMV

-- xlate :: (f k a -> g k a) -> Btree f k a -> Btree g k a 
-- xlate f (Btree a ks) = Btree a (f ks)

-- mkbmv :: Int -> [(Int,Int)] -> BMV
-- mkbmv a xs = BMV (Btree a (Map.fromList xs))

-- make :: Int -> Tree
-- make n | n <= 0 = Empty
-- make n =  Tree n (make (n `div` 2)) (make ((n - 1) `div` 2)) 

-- crack :: Int -> Int -> Crack Int
-- crack a b = Crack a  (a + b) (VecU.generate 0xFFF (\i -> (i+a,i+b)))

-- main :: IO ()
-- main = flip mapM_ ([1..0xFFFFF]::[Int]) $ \i -> 
--     -- ByteString.putStr $ SerP.runPut $ Ser.put $ map (i+) $ [1..1000]
--     -- ByteString.putStr $ SerP.runPut $ VecU.mapM_ Ser.put $ VecU.generate 1000 (\j -> i + j)
--     -- ByteStringL.putStr $ BinP.runPut $ VecU.mapM_ Bin.put $ VecU.generate 1000 (\j -> i + j)
--     -- ByteString.putStr $ Store.encode $ VecU.generate 1000000 (\j -> i + j)
--     -- ByteString.putStr $ Store.encode $ crack 4 i
--     ByteString.putStr $ Store.encode $ BUV $ xlate (UVMap . VecU.fromList . Map.toList) $ bmv $ mkbmv i $ zip [1..] $ replicate 0xFFF i

-- data Node v k a = Node (v (k,a))

-- class VecG.Vector v (k,a) => N v k a 


-- eq :: (VecG.Vector v (k,a), Ord k) => Node v k a -> k -> Maybe a
-- eq (Node v) k = go 0 (VecG.length v - 1)
--     where
--     go lo hi | lo > hi = Nothing
--              | otherwise =  let mid = (lo + hi) `div` 2 
--                                 (mk,mv) = v VecG.! mid
--                                 in 
--                             case k `compare` mk of
--                             LT -> go lo (mid - 1)
--                             EQ -> Just mv
--                             GT -> go (mid + 1) hi

-- minMonotonic :: (VecG.Vector v k) => (k -> Bool) -> v k -> Maybe k
-- minMonotonic pred v = go 0 (VecG.length v - 1)
--     where
--     go lo hi = 
--         let mid = (lo + hi + 1) `div` 2 
--             k = v VecG.! mid
--         in 
--         case lo `compare` hi of
--             GT -> Nothing
--             EQ -> if pred k then Just k else Nothing
--             LT -> if pred k then go mid hi else go lo (mid - 1)


-- findLE :: (Ord k, N v k a) => k -> Node v k a -> Maybe (k,a)
-- findLE k (Node v) =  minMonotonic (\(k',a) -> k' <= k) v

-- findEQ :: (Ord k, N v k a) => k -> Node v k a -> Maybe a
-- findEQ k node = let (k',v) = findLE node 

-- data Tree v k p a = TNode (Node v k p) | TLeaf (Node v k a)

-- find :: Monad m => (forall a . (N v k a) => k -> Node v k a -> Maybe (k,a)) 
--                 -> (ptr -> m (Tree v k ptr a)) 
--                 -> ptr
--                 -> k
--                 -> m (Maybe a)
-- find le get ptr key = go ptr key
--     where
--     go ptr key = do
--         get ptr >>= \case
--             TNode node -> 
