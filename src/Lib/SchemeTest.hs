module Lib.SchemeTest where

import Lib.Schemes as Sch
import Streaming.Prelude as StP
import Data.Fix (Fix(..))
import Data.Functor.Compose (Compose(..))
import qualified Control.Monad.State.Strict as State
import Control.Monad.Identity (Identity, runIdentity)
import Control.Monad.Trans.Identity (runIdentityT)

-- TODO: Get the args in the right order for foldr/foldl

streamBST :: Monad m => Fix (Compose m (BT a)) -> Stream (Of a) m ()
streamBST bt = rfoldr bt (\a () -> StP.yield a) ()

huge :: Int -> Fix (Compose IO (BT Int))
huge depth = go 0 1
    where
    go :: Int -> Int -> Fix (Compose IO (BT Int))
    go d n | d == depth = Sch.l_
           | otherwise = Sch.n_ n (go (d + 1) (n * 2)) (go (d + 1) (n * 2 + 1))

hugeS :: Int -> Fix (Compose (State.State Int) (BT Int))
hugeS depth = go 0
    where
    go :: Int  -> Fix (Compose (State.State Int) (BT Int))
    go d | d == depth = Sch.l_
         | otherwise = Fix $ Compose $ do
                n <- State.state (\i -> (i, i + 1))
                return $ Sch.N n (go (d + 1)) (go (d + 1))

hugeI :: Int -> Fix (Compose Identity (BT Int))
hugeI depth = go 0
    where
    go :: Int  -> Fix (Compose Identity (BT Int))
    go d | d == depth = Sch.l_
         | otherwise = Fix $ Compose $ do
                -- n <- State.state (\i -> (i, i + 1))
                return $ Sch.N 0 (go (d + 1)) (go (d + 1))

test :: Int -> IO ()
test n = Prelude.print =<< (StP.length $ streamBST $ huge n)

testS :: Int -> IO ()
testS n = Prelude.print $ flip State.runState 0 $ StP.length $ streamBST $ hugeS n


test2S :: Int -> IO ()
test2S n = Prelude.print $ flip State.runState 0 $ runIdentityT $ (rfoldl' ( hugeS n)  (\acc x -> return (x + acc)) 0)

testI :: Int -> IO ()
testI n = Prelude.print $ runIdentity $ StP.length $ streamBST $ hugeI n

test2I :: Int -> IO ()
test2I n = Prelude.print $ runIdentity $ runIdentityT $ (rfoldl'  ( hugeI n) (\acc _ -> return ((1 :: Int) + acc)) 0) 





