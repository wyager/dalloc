module Lib.Structures.Schemes.Example where

import Lib.Structures.Schemes as Sch
import Streaming.Prelude as StP
import Data.Fix (Fix(..))
import Data.Functor.Compose (Compose(..))
import qualified Control.Monad.State.Strict as State
import Control.Monad.Identity (Identity, runIdentity)
import Control.Monad.Trans.Identity (runIdentityT)


data BT a r = N a r r | L

instance FoldFix BT where
  {-# INLINABLE rfoldr_ #-}
  rfoldr_ rec = \f b0 g -> case g of
    N a gl gr -> do
      br <- rec f b0 gr
      bm <- f a br
      bl <- rec f bm gl
      return bl
    L -> return b0
  {-# INLINABLE rfoldl'_ #-}
  rfoldl'_ rec = \f b0 g -> case g of
    N a gl gr -> do
      !bl <- rec f b0 gl
      !bm <- f bl a
      !br <- rec f bm gr
      return br
    L -> return b0



l_ :: Applicative m => Fix (Compose m (BT a))
l_ = Fix (Compose (pure L))

n_
  :: Applicative m
  => a
  -> Fix (Compose m (BT a))
  -> Fix (Compose m (BT a))
  -> Fix (Compose m (BT a))
n_ a l r = Fix (Compose (pure (N a l r)))




streamBST :: Monad m => Fix (Compose m (BT a)) -> Stream (Of a) m ()
streamBST = rfoldr (\a () -> StP.yield a) ()

huge :: Int -> Fix (Compose IO (BT Int))
huge depth = go 0 1
 where
  go :: Int -> Int -> Fix (Compose IO (BT Int))
  go d n
    | d == depth = l_
    | otherwise  = n_ n (go (d + 1) (n * 2)) (go (d + 1) (n * 2 + 1))

hugeS :: Int -> Fix (Compose (State.State Int) (BT Int))
hugeS depth = go 0
 where
  go :: Int -> Fix (Compose (State.State Int) (BT Int))
  go d
    | d == depth = l_
    | otherwise = Fix $ Compose $ do
      n <- State.state (\i -> (i, i + 1))
      return $ N n (go (d + 1)) (go (d + 1))

hugeI :: Int -> Fix (Compose Identity (BT Int))
hugeI depth = go 0
 where
  go :: Int -> Fix (Compose Identity (BT Int))
  go d
    | d == depth = l_
    | otherwise = Fix $ Compose $ do
              -- n <- State.state (\i -> (i, i + 1))
      return $ N 0 (go (d + 1)) (go (d + 1))

test :: Int -> IO ()
test n = Prelude.print =<< (StP.length $ streamBST $ huge n)

testS :: Int -> IO ()
testS n = Prelude.print $ flip State.runState 0 $ StP.length $ streamBST $ hugeS n


test2S :: Int -> IO ()
test2S n =
  Prelude.print
    $ flip State.runState 0
    $ runIdentityT
    $ (rfoldl' (\acc x -> return (x + acc)) 0 (hugeS n))

testI :: Int -> IO ()
testI n = Prelude.print $ runIdentity $ StP.length $ streamBST $ hugeI n

test2I :: Int -> IO ()
test2I n =
  Prelude.print
    $ runIdentity
    $ runIdentityT
    $ (rfoldl' (\acc _ -> return ((1 :: Int) + acc)) 0 (hugeI n))





