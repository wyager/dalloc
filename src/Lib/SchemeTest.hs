module Lib.SchemeTest where

import Lib.Schemes as Sch
import Streaming.Prelude as StP
import Data.Fix (Fix(..))
import Data.Functor.Compose (Compose(..))
import qualified Control.Monad.State.Strict as State
import Control.Monad.Identity (Identity, runIdentity)
import Control.Monad.Trans.Identity (runIdentityT)
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Compose (ComposeT(..))
import Control.Monad.Trans.Identity (IdentityT(..))



streamBST :: Monad m => Fix (Compose m (BT a)) -> Stream (Of a) m ()
streamBST = Sch.unstack (foldMR () (\a () -> StP.yield a)) 

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
test2S n = Prelude.print $ flip State.runState 0 $ runIdentityT $ unstack (foldML' 0 (\acc x -> return (x + acc))) $ hugeS n

testI :: Int -> IO ()
testI n = Prelude.print $ runIdentity $ StP.length $ streamBST $ hugeI n

test2I :: Int -> IO ()
test2I n = Prelude.print $ runIdentity $ runIdentityT $ unstack (foldML' 0 (\acc _ -> return ((1 :: Int) + acc))) $ hugeI n

streamBST2 :: Monad m => Fix (Compose m (BT a)) -> Stream (Of a) m ()
streamBST2 bt = rfoldr bt (\a () -> StP.yield a) ()

lenBST2 :: Monad m => Fix (Compose m (BT a)) -> m Int
lenBST2 bt = runIdentityT $ rfoldl' bt (\acc _ -> return (acc + 1)) 0

test3 :: Int -> IO ()
test3 n = Prelude.print =<< StP.length (streamBST2 (huge n))

test4 :: Int -> IO ()
test4 n = Prelude.print $ runIdentity (lenBST2 (hugeI n))

-- y :: _
-- y = Sch.unstack2 Sch.foldMR2

-- x :: Monad m => Fix (Compose m (BT a)) -> (a -> b -> m b) -> b -> m b -- Monad m => Fix (Compose m (BT a)) -> MFoldR a b m b -- Monad m => Fix (Compose m (BT a)) -> (a -> b -> m b) -> b -> m b
-- x bt = Sch.runMFoldR (Sch.unstack2 Sch.foldMR2 bt)



