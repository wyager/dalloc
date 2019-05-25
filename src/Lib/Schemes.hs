module Lib.Schemes where

import Control.Monad.Trans.Class (MonadTrans, lift)
import Data.Fix (Fix(..))
import Data.Functor.Compose (Compose(..))
import Control.Monad ((<=<))
import Control.Monad.Trans.Reader (ReaderT(..))


data BT a r = N a r r | L


l_ :: Applicative m => Fix (Compose m (BT a))
l_ = Fix (Compose (pure L))

n_ :: Applicative m => a -> Fix (Compose m (BT a)) -> Fix (Compose m (BT a)) -> Fix (Compose m (BT a))
n_ a l r = Fix (Compose (pure (N a l r)))



newtype MFoldR a b t (m :: * -> *) r = MFoldR {runMFoldR :: (a -> b -> t m b) -> b -> t m r }
    deriving (Functor, Applicative, Monad) via ReaderT (a -> b -> t m b) (ReaderT b (t m)) 

instance MonadTrans t => MonadTrans (MFoldR a b t) where
    lift m = MFoldR $ \ _ _ -> lift m

newtype MFoldL a b t (m :: * -> *) r = MFoldL {runMFoldL :: (b -> a -> t m b) -> b -> t m r }
    deriving (Functor, Applicative, Monad) via ReaderT (b -> a -> t m b) (ReaderT b (t m)) 

instance MonadTrans t => MonadTrans (MFoldL a b t) where
    lift m = MFoldL $ \ _ _ -> lift m


class FoldFix (g :: * -> * -> *) where
    rfoldr_ :: forall t a b . () 
           => (forall m . Monad m => Monad (t m)) 
           => MonadTrans t
           => forall m . Monad m 
           => forall z . ((a -> b -> t m b) -> b -> z -> t m b)
           -> (a -> b -> t m b) -> b -> g a z -> t m b
    rfoldl'_ :: forall a b t . () 
           => (forall m . Monad m => Monad (t m)) 
           => MonadTrans t
           => forall m . Monad m 
           => forall z . ((b -> a -> t m b) -> b -> z -> t m b)
           -> (b -> a -> t m b) -> b -> g a z -> t m b


instance FoldFix BT where
    rfoldr_ rec = \f b0 g -> case g of
            N a gl gr-> do
                br <- rec f b0 gr
                bm <- f a br
                bl <- rec f bm gl
                return bl
            L -> return b0
    rfoldl'_ rec = \f b0 g -> case g of
            N a gl gr-> do
                !bl <- rec f b0 gl
                !bm <- f bl a
                !br <- rec f bm gr
                return br
            L -> return b0


unstack :: forall t m a g  . ()
        => Monad m 
        => (forall n . Monad n => Monad (t n))
        => MonadTrans t
        => (forall n z . Monad n => (z -> t n a) -> g z -> t n a)
        -> Fix (Compose m g) -> t m a
unstack f = go
    where
    go :: Fix (Compose m g) -> t m a
    go = f go <=< lift . getCompose . unFix 

-- I would like to thank the GHC optimizer
-- This annoying stuff is due to unstack imposing a different order than the standard fold argument order
rfoldr :: forall a b g m t. (FoldFix g, Monad m, MonadTrans t, (forall n . Monad n => Monad (t n))) => (a -> b -> t m b) -> b -> Fix (Compose m (g a)) -> t m b
rfoldr f b0 g = runMFoldR (unstack step g) f b0
  where
  step :: forall z n . Monad n => (z -> MFoldR a b t n b) -> g a z -> MFoldR a b t n b
  step zf gaz = MFoldR $ \abt b -> rfoldr_ (\abt' b' z -> runMFoldR (zf z) abt' b') abt b gaz

rfoldl' :: forall a b g m t. (FoldFix g, Monad m, MonadTrans t, (forall n . Monad n => Monad (t n))) => (b -> a -> t m b) -> b -> Fix (Compose m (g a)) -> t m b
rfoldl' f b0 g = runMFoldL (unstack step g) f b0
  where
  step :: forall z n . Monad n => (z -> MFoldL a b t n b) -> g a z -> MFoldL a b t n b
  step zf gaz = MFoldL $ \bat b -> rfoldl'_ (\bat' b' z -> runMFoldL (zf z) bat' b') bat b gaz
