{-# LANGUAGE UndecidableInstances #-} -- for NFData FixN
module Lib.Structures.Schemes where

import Control.Monad.Trans.Class (MonadTrans, lift)
import Data.Fix (Fix(..))
import Data.Functor.Compose (Compose(..))
import Control.Monad.Trans.Reader (ReaderT(..))
import GHC.Exts (Constraint)


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
unstack :: forall t m a g r . ()
        => Monad m 
        => (forall n . Monad n => Monad (t n))
        => MonadTrans t
        => (forall x . r x -> m x)
        -> (forall n z . Monad n => (z -> t n a) -> g z -> t n a)
        -> Fix (Compose r g) -> t m a
unstack r2m f = go
    where
    go :: Fix (Compose r g) -> t m a
    go (Fix (Compose rg)) = f go     =<<(lift (r2m rg))

-- I would like to thank the GHC optimizer
-- This annoying stuff is due to unstack imposing a different order than the standard fold argument order
{-# INLINEABLE rfoldr #-}
rfoldr :: forall a b g h m t r . (FoldFix h, Monad m, MonadTrans t, (forall n . Monad n => Monad (t n))) => (forall x . r x -> m x) -> (forall x . g x -> h a x) -> (a -> b -> t m b) -> b -> Fix (Compose r g) -> t m b
rfoldr r2m g2h f b0 g = runMFoldR (unstack r2m step g) f b0
  where
  step :: forall z n . Monad n => (z -> MFoldR a b t n b) -> g z -> MFoldR a b t n b
  step zf gaz = MFoldR $ \abt b -> rfoldr_ (\abt' b' z -> runMFoldR (zf z) abt' b') abt b (g2h gaz) -- Try writing a version that unwraps z


{-# INLINEABLE rfoldl' #-}
rfoldl' :: forall a b g h m t r . (FoldFix h, Monad m, MonadTrans t, (forall n . Monad n => Monad (t n))) => (forall x . r x -> m x) -> (forall x . g x -> h a x) -> (b -> a -> t m b) -> b -> Fix (Compose r g) -> t m b
rfoldl' r2m g2h f b0 g = runMFoldL (unstack r2m step g) f b0
  where
  step :: forall z n . Monad n => (z -> MFoldL a b t n b) -> g z -> MFoldL a b t n b
  step zf gaz = MFoldL $ \bat b -> rfoldl'_ (\bat' b' z -> runMFoldL (zf z) bat' b') bat b (g2h gaz)




type Lift c f = (forall s . c s => c (f s) :: Constraint)


