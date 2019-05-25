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
           => forall z . (z -> (a -> b -> t m b) -> b -> t m b)
           -> g a z
           -> (a -> b -> t m b) -> b -> t m b
    rfoldl'_ :: forall a b t . () 
           => (forall m . Monad m => Monad (t m)) 
           => MonadTrans t
           => forall m . Monad m 
           => forall z . (z -> (b -> a -> t m b) -> b -> t m b)
           -> g a z
           -> (b -> a -> t m b) -> b -> t m b


instance FoldFix BT where
    rfoldr_ rec g = \f b0 -> case g of
            N a gl gr-> do
                br <- rec gr f b0
                bm <- f a br
                bl <- rec gl f bm
                return bl
            L -> return b0
    rfoldl'_ rec g = \f b0 -> case g of
            N a gl gr-> do
                !bl <- rec gl f b0
                !bm <- f bl a
                !br <- rec gr f bm
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


rfoldr :: forall a b g m t. (FoldFix g, Monad m, MonadTrans t, (forall n . Monad n => Monad (t n))) => Fix (Compose m (g a)) -> (a -> b -> t m b) -> b -> t m b
rfoldr   = runMFoldR . unstack (\zf -> MFoldR . rfoldr_ (runMFoldR . zf)) 

rfoldl' ::  forall a b g m t. (FoldFix g, Monad m, MonadTrans t, (forall n . Monad n => Monad (t n))) => Fix (Compose m (g a)) -> (b -> a -> t m b) -> b -> t m b
rfoldl'   = runMFoldL . unstack (\zf -> MFoldL . rfoldl'_ (runMFoldL . zf)) 


