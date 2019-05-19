module Lib.Schemes where

import Control.Monad.Trans.Class (MonadTrans, lift)
import Data.Fix (Fix(..))
import Data.Functor.Compose (Compose(..))
import Control.Monad.Trans.Identity (IdentityT(..))
import Control.Monad ((<=<))
import Control.Monad.Trans.Reader (ReaderT(..))
import Data.Coerce (coerce)
import Control.Monad.Trans.Compose (ComposeT(..))
import Control.Monad.Morph (MFunctor(hoist))

unstack :: forall a g t m . ()
        => Monad m 
        => (forall n . Monad n => Monad (t n))
        => MonadTrans t
        => (forall n . Monad n => g (Fix (Compose n g)) -> t n a)
        -> Fix (Compose m g) -> t m a
unstack f = f <=< lift . getCompose . unFix 

class FoldFix g where
    foldMR :: forall a b t . () 
           => (forall m . Monad m => Monad (t m)) 
           => MonadTrans t
           => b
           -> (forall m . Monad m => a -> b -> t m b)
           -> forall m . Monad m 
           => g a (Fix (Compose m (g a)))
           -> t m b
    foldML' :: forall a b t . () 
           => (forall m . Monad m => Monad (t m)) 
           => MonadTrans t
           => b
           -> (forall m . Monad m => b -> a -> t m b)
           -> forall m . Monad m 
           => g a (Fix (Compose m (g a)))
           -> t m b

data BT a r = N a r r | L

instance FoldFix BT where
    foldMR b0 f g = case g of
        N a (Fix (Compose gl)) (Fix (Compose gr)) -> do
            br <- lift gr >>= foldMR b0 f
            bm <- f a br
            bl <- lift gl >>= foldMR bm f 
            return bl
        L -> return b0
    foldML' b0 f g = case g of
        N a (Fix (Compose gl)) (Fix (Compose gr)) -> do
            !bl <- lift gl >>= foldML' b0 f 
            !bm <- f bl a
            !br <- lift gr >>= foldML' bm f
            return br
        L -> return b0
        

mList :: (Monad m, MonadTrans t, (forall n . Monad n => Monad (t n))) => Fix (Compose m (BT a)) -> t m [a]
mList = unstack (foldMR [] (\hd tl -> return (hd : tl)))

l_ :: Applicative m => Fix (Compose m (BT a))
l_ = Fix (Compose (pure L))

n_ :: Applicative m => a -> Fix (Compose m (BT a)) -> Fix (Compose m (BT a)) -> Fix (Compose m (BT a))
n_ a l r = Fix (Compose (pure (N a l r)))

test :: Monad m => m [Int]
test = runIdentityT $ mList (n_ 5 (n_ 1 l_ l_) (n_ 7 l_ l_))


-- NB: The reason behding using MFoldR is that it simplifies the signature of a generic
-- unstack function, where we only need to have a single argument corresponding to the
-- recursive data structure (z) rather than one argument for each argument in the recursive function.
-- However, streamBST2 is giving me trouble by requiring an MFunctor instance

newtype MFoldR a b (m :: * -> *) r = MFoldR {runMFoldR :: (a -> b -> m b) -> b -> m r }
    deriving (Functor, Applicative, Monad) via ReaderT (a -> b -> m b) (ReaderT b m) 

-- instance MFunctor (MFoldR a b) where
--     hoist f (MFoldR g) = MFoldR $ \step base -> let step' = \a b -> f (step a b) in g step' base

-- How do we abstract away recursion? foldMR shouldn't have to call itself.
foldMR2 :: forall a b t . () 
           => (forall m . Monad m => Monad (t m)) 
           => MonadTrans t
           => forall m . Monad m 
           => forall z . (z -> MFoldR a b (t m) b)
           -> BT a z
           -> MFoldR a b (t m) b
foldMR2 rec g = MFoldR $ \f b0 -> case g of
        N a gl gr-> do
            br <- runMFoldR (rec gl) f b0
            bm <- f a br
            bl <- runMFoldR (rec gr) f bm
            return bl
        L -> return b0

foldMR2' :: forall a b t . () 
           => (forall m . Monad m => Monad (t m)) 
           => MonadTrans t
           => forall m . Monad m 
           => forall z . (z -> ComposeT (MFoldR a b) t m b)
           -> BT a z
           -> ComposeT (MFoldR a b) t m b
foldMR2' rec g = let rec' = getComposeT . rec in ComposeT (foldMR2 rec' g)

unstack2 :: forall a g t m . ()
        => Monad m 
        => (forall n . Monad n => Monad (t n))
        => MonadTrans t
        => (forall n z . Monad n => (z -> t n a) -> g z -> t n a)
        -> Fix (Compose m g) -> t m a
unstack2 f = go
    where
    go :: Fix (Compose m g) -> t m a
    go  = f go <=< lift . getCompose . unFix









