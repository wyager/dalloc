{-# LANGUAGE UndecidableInstances #-} -- for NFData FixN
module Lib.Structures.Schemes where

import Control.Monad.Trans.Class (MonadTrans, lift)
import Data.Fix (Fix(..))
import Data.Functor.Compose (Compose(..))
import Control.Monad ((<=<))
import Control.Monad.Trans.Reader (ReaderT(..))
import Data.Void (Void, absurd, vacuous)
import GHC.Exts (Constraint)
import Control.DeepSeq (NFData, rnf)
import qualified Data.Store as S
import Data.Functor.Contravariant (contramap)
import Data.Proxy (Proxy(..))


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
    go :: forall j . Fix (Compose r g) -> t m a
    go (Fix (Compose rg)) = f go     =<<(lift (r2m rg))

-- I would like to thank the GHC optimizer
-- This annoying stuff is due to unstack imposing a different order than the standard fold argument order
{-# INLINEABLE rfoldr #-}
rfoldr :: forall a b g h m t j r . (FoldFix h, Monad m, MonadTrans t, (forall n . Monad n => Monad (t n))) => (forall x . r x -> m x) -> (forall x . g x -> h a x) -> (a -> b -> t m b) -> b -> Fix (Compose r g) -> t m b
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
data FixN (n :: Nat) f where
    FixZ :: !(f  'Z     Void)      -> FixN  'Z    f
    FixN :: !(f ('S n) (FixN n f)) -> FixN ('S n) f


class StoreN n a where
  sizeN :: proxy n -> S.Size a
  peekN :: proxy n -> S.Peek a
  pokeN :: proxy n -> a -> S.Poke ()


-- type family FixNIx a :: Nat where
--   FixNIx (FixN n _) = n
  -- FixNIx (FixN ('S n) _) = 'S n


instance (S.Store (f 'Z Void)) => StoreN 'Z (FixN 'Z f) where
  sizeN _ = contramap (\(FixZ f) -> f) S.size
  peekN _ = FixZ <$> S.peek
  pokeN _ = S.poke . (\(FixZ f) -> f)


instance (forall m r . S.Store r => S.Store (f m r), StoreN n (FixN n f)) => StoreN ('S n) (FixN ('S n) f) where
  sizeN _ = contramap (\(FixN f) -> f) S.size
  peekN _ = FixN <$> S.peek
  pokeN _ = S.poke . (\(FixN f) -> f)

instance (StoreN n (FixN n f), forall r n . S.Store r => S.Store (f n r)) => S.Store (FixN n f) where
  size = sizeN (Proxy @n) 
  peek = peekN (Proxy @n) 
  poke = pokeN (Proxy @n) 


newtype Const f (n :: Nat) a = Const (f a)
  deriving newtype S.Store

newtype Test = Test (FixN ('S 'Z) (Const [])) 
  deriving newtype S.Store


-- instance (forall m r . S.Store r => S.Store (f m r)) => S.Store (FixN n f) where
--   size = S.VarSize $ \case
--           FixZ f -> case S.size @(f 'Z Void) of
--             S.ConstSize x -> x
-- instance (forall m r . S.Store r => S.Store (f m r))  => S.Store (FixN 'Z f) where
--   size = contramap (\(FixZ f) -> f) S.size
--   peek = FixZ <$> S.peek
--   poke = S.poke . (\(FixZ f) -> f)

-- instance (forall m r . S.Store r => S.Store (f m r), S.Store (FixN n f)) => S.Store (FixN ('S n) f) where
--   size = contramap (\(FixN f) -> f) S.size
--   peek = FixN <$> S.peek
--   poke = S.poke . (\(FixN f) -> f)


instance (forall j a . NFData a => NFData (f j a)) => NFData (FixN i f) where 
  rnf (FixZ f) = rnf f
  rnf (FixN f) = rnf f

newtype ComposeI (f :: * -> *) (g :: Nat -> * -> *) (n :: Nat) (a :: *)  = ComposeI {getComposeI :: f (g n a)}
  deriving newtype NFData
  deriving S.Store via (f (g n a))

instance (Functor f, Functor (g n)) => Functor (ComposeI f g n) where
  fmap q (ComposeI f) = ComposeI $ fmap (fmap q) f



type Lift c f = (forall s . c s => c (f s) :: Constraint)



cataNM :: (forall n' . Traversable (f n'), Monad m) => (forall n' . f n' a -> m a) -> FixN n f -> m a
cataNM g (FixZ f) = g (vacuous f)
cataNM g (FixN f) = g =<< (traverse (cataNM g) f)


{-# INLINE unstackI #-}
unstackI :: forall t m a g i r . ()
        => Monad m 
        => (forall n . Monad n => Monad (t n))
        => MonadTrans t
        => (forall x . r x -> m x)
        -> (forall n z j . Monad n => (z -> t n a) -> g j z -> t n a)
        -> FixN i (ComposeI r g) -> t m a
unstackI r2m f = go
    where
    go :: forall j . FixN j (ComposeI r g) -> t m a
    go (FixZ (ComposeI rg)) = f absurd =<<(lift (r2m rg))
    go (FixN (ComposeI rg)) = f go     =<<(lift (r2m rg))


{-# INLINEABLE rfoldri #-}
rfoldri :: forall a b g h m t j r . (FoldFixI h, Monad m, MonadTrans t, (forall n . Monad n => Monad (t n))) => (forall x . r x -> m x) -> (forall i x . g i x -> h a i x) -> (a -> b -> t m b) -> b -> FixN j (ComposeI r g) -> t m b
rfoldri r2m g2h f b0 g = runMFoldR (unstackI r2m step g) f b0
  where
  step :: forall z n j' . Monad n => (z -> MFoldR a b t n b) -> g j' z -> MFoldR a b t n b
  step zf gaz = MFoldR $ \abt b -> rfoldri_ (\abt' b' z -> runMFoldR (zf z) abt' b') abt b (g2h gaz) -- Try writing a version that unwraps z


{-# INLINEABLE rfoldli' #-}
rfoldli' :: forall a b g h m t j r . (FoldFixI h, Monad m, MonadTrans t, (forall n . Monad n => Monad (t n))) => (forall x . r x -> m x) -> (forall i x . g i x -> h a i x) -> (b -> a -> t m b) -> b -> FixN j (ComposeI r g) -> t m b
rfoldli' r2m g2h f b0 g = runMFoldL (unstackI r2m step g) f b0
  where
  step :: forall z n j' . Monad n => (z -> MFoldL a b t n b) -> g j' z -> MFoldL a b t n b
  step zf gaz = MFoldL $ \bat b -> rfoldli'_ (\bat' b' z -> runMFoldL (zf z) bat' b') bat b (g2h gaz)
