module Lib.Schemes where

import Control.Monad.Trans.Class (MonadTrans, lift)
import Data.Fix (Fix(..))
import Data.Functor.Compose (Compose(..))
import Control.Monad ((<=<))
import Control.Monad.Trans.Reader (ReaderT(..))
import Data.Void (Void, absurd, vacuous)





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






{-# INLINE unstack #-}
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
{-# INLINEABLE rfoldr #-}
rfoldr :: forall a b g m t. (FoldFix g, Monad m, MonadTrans t, (forall n . Monad n => Monad (t n))) => (a -> b -> t m b) -> b -> Fix (Compose m (g a)) -> t m b
rfoldr f b0 g = runMFoldR (unstack step g) f b0
  where
  step :: forall z n . Monad n => (z -> MFoldR a b t n b) -> g a z -> MFoldR a b t n b
  step zf gaz = MFoldR $ \abt b -> rfoldr_ (\abt' b' z -> runMFoldR (zf z) abt' b') abt b gaz


{-# INLINEABLE rfoldl' #-}
rfoldl' :: forall a b g m t. (FoldFix g, Monad m, MonadTrans t, (forall n . Monad n => Monad (t n))) => (b -> a -> t m b) -> b -> Fix (Compose m (g a)) -> t m b
rfoldl' f b0 g = runMFoldL (unstack step g) f b0
  where
  step :: forall z n . Monad n => (z -> MFoldL a b t n b) -> g a z -> MFoldL a b t n b
  step zf gaz = MFoldL $ \bat b -> rfoldl'_ (\bat' b' z -> runMFoldL (zf z) bat' b') bat b gaz

class FoldFixI (g :: * -> Nat -> * -> *) where
    rfoldri_ :: forall t a b j . () 
           => (forall m . Monad m => Monad (t m)) 
           => MonadTrans t
           => forall m . Monad m 
           => forall z . ((a -> b -> t m b) -> b -> z -> t m b)
           -> (a -> b -> t m b) -> b -> g a j z -> t m b
    rfoldli'_ :: forall a b t j . () 
           => (forall m . Monad m => Monad (t m)) 
           => MonadTrans t
           => forall m . Monad m 
           => forall z . ((b -> a -> t m b) -> b -> z -> t m b)
           -> (b -> a -> t m b) -> b -> g a j z -> t m b


data Nat = Z | S Nat
data FixN n f where
    FixZ :: f  'Z     Void      -> FixN  'Z     f
    FixN :: f ('S n) (FixN n f) -> FixN ('S n) f
newtype ComposeI (f :: * -> *) (g :: Nat -> * -> *) (n :: Nat) (a :: *)  = ComposeI {getComposeI :: f (g n a)}


cataNM :: (forall n' . Traversable (f n'), Monad m) => (forall n' . f n' a -> m a) -> FixN n f -> m a
cataNM g (FixZ f) = g (vacuous f)
cataNM g (FixN f) = g =<< (traverse (cataNM g) f)

{-# INLINE unstackI #-}
unstackI :: forall t m a g i  . ()
        => Monad m 
        => (forall n . Monad n => Monad (t n))
        => MonadTrans t
        => (forall n z j . Monad n => (z -> t n a) -> g j z -> t n a)
        -> FixN i (ComposeI m g) -> t m a
unstackI f = go
    where
    go :: forall j . FixN j (ComposeI m g) -> t m a
    go (FixZ (ComposeI mg)) = f absurd =<< lift mg
    go (FixN (ComposeI mg)) = f go     =<< lift mg



{-# INLINEABLE rfoldri #-}
rfoldri :: forall a b g m t j . (FoldFixI g, Monad m, MonadTrans t, (forall n . Monad n => Monad (t n))) => (a -> b -> t m b) -> b -> FixN j (ComposeI m (g a)) -> t m b
rfoldri f b0 g = runMFoldR (unstackI step g) f b0
  where
  step :: forall z n j' . Monad n => (z -> MFoldR a b t n b) -> g a j' z -> MFoldR a b t n b
  step zf gaz = MFoldR $ \abt b -> rfoldri_ (\abt' b' z -> runMFoldR (zf z) abt' b') abt b gaz

{-# INLINEABLE rfoldli' #-}
rfoldli' :: forall a b g m t j . (FoldFixI g, Monad m, MonadTrans t, (forall n . Monad n => Monad (t n))) => (b -> a -> t m b) -> b -> FixN j (ComposeI m (g a)) -> t m b
rfoldli' f b0 g = runMFoldL (unstackI step g) f b0
  where
  step :: forall z n j' . Monad n => (z -> MFoldL a b t n b) -> g a j' z -> MFoldL a b t n b
  step zf gaz = MFoldL $ \bat b -> rfoldli'_ (\bat' b' z -> runMFoldL (zf z) bat' b') bat b gaz
