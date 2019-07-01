-- Attempting to reconcile depth-typed GiST with Schemes

{-# LANGUAGE UndecidableInstances #-} -- Show constraints
{-# OPTIONS_GHC -fno-warn-orphans #-}

module Lib.GiSTf2 where

-- import GHC.TypeLits (Nat, type (+))
import qualified Data.Vector as V (Vector)-- , fromList)
import Data.Vector.Generic as Vec (Vector, toList, filter, foldM, concat) --, concat, filter, toList, imap, ifoldl, singleton, length, fromList, tail, splitAt, empty, foldM, span)
import qualified Data.Vector.Generic as Vec (foldr, foldl') --, concat, filter, toList, imap, ifoldl, singleton, length, fromList, tail, splitAt, empty, foldM, span)
import Data.Monoid (Endo(..))
-- import Data.Semigroup (Min(Min,getMin))
import qualified Data.Foldable as Foldable (foldrM, foldlM)
-- import Control.Monad.Free (Free(Free,Pure))
import Data.Functor.Compose (Compose)
-- import Data.Proxy (Proxy(Proxy))
import Data.Functor.Identity (Identity(..))
import qualified Data.Vector.Unboxed as U
import Lib.Schemes (Nat(S,Z), FixN(FixZ,FixN),ComposeI(..),FoldFixI,Lift, rfoldri_, rfoldli'_, rfoldli', rfoldri, hoistNL, swozzle)
import Control.Monad.Trans.Identity (runIdentityT)
import Control.Monad.Trans.Class (MonadTrans)
import Data.Functor.Sum (Sum(InL,InR))
-- import Data.Void (Void)

-- import qualified Lib.Schemes as S

-- data Nat = Z | S Nat

data AFew xs = One xs | Two xs xs deriving (Functor, Foldable, Traversable)

data V2 x = V2 x x

type f ∘ g = Compose f g


instance (forall n' . Lift Show (f n')) => Show (FixN n f) where
    show (FixZ f) = show f
    show (FixN f) = show f

-- data FixN n f where
--     FixZ :: f  'Z     Void      -> FixN  'Z     f
--     FixN :: f ('S n) (FixN n f) -> FixN ('S n) f

-- type LiftI c f = (forall n s . c s => c (f n s) :: Constraint)






-- class FoldFixI (g :: * -> Nat -> * -> *) where
--     rfoldri_ :: forall t a b j . () 
--            => (forall m . Monad m => Monad (t m)) 
--            => MonadTrans t
--            => forall m . Monad m 
--            => forall z . ((a -> b -> t m b) -> b -> z -> t m b)
--            -> (a -> b -> t m b) -> b -> g a j z -> t m b
--     rfoldli'_ :: forall a b t j . () 
--            => (forall m . Monad m => Monad (t m)) 
--            => MonadTrans t
--            => forall m . Monad m 
--            => forall z . ((b -> a -> t m b) -> b -> z -> t m b)
--            -> (b -> a -> t m b) -> b -> g a j z -> t m b


data FoldWithoutKey vec set key value n rec where
    FoldWithoutKey :: Vector vec (key,value) => GiSTr vec set key value n rec -> FoldWithoutKey vec set key value n rec

data FoldWithKey vec set kv n rec where
    FoldWithKey :: (Vector vec kv, kv ~ (k,v)) => GiSTr vec set k v n rec -> FoldWithKey vec set kv n rec


data FoldWithVec set vec_kv n rec where
    FoldWithVec :: (Vector vec kv, vec_kv ~ vec kv, kv ~ (k,v)) => GiSTr vec set k v n rec -> FoldWithVec set vec_kv n rec


instance FoldFixI (FoldWithVec set) where
    {-# INLINEABLE rfoldri_ #-}
    rfoldri_ rec = \f b0 (FoldWithVec g) -> case g of
        Leaf vec -> f vec b0
        Node vec -> Foldable.foldrM go b0 vec
            where
            go (_set,subtree) acc = rec f acc subtree
    {-# INLINEABLE rfoldli'_ #-}
    rfoldli'_ rec = \f b0 (FoldWithVec g) -> case g of
        Leaf vec -> f b0 vec 
        Node vec -> Foldable.foldlM go b0 vec
            where
            go  acc (_set,subtree) = rec f acc subtree

instance FoldFixI (FoldWithoutKey vec set key) where
    {-# INLINEABLE rfoldri_ #-}
    rfoldri_ rec = \f b0 (FoldWithoutKey g) -> case g of
        Leaf vec -> Vec.foldr (\(_k,v) acc -> f v =<< acc) (return b0) vec -- 
        Node vec -> Foldable.foldrM go b0 vec
            where
            go (_set,subtree) acc = rec f acc subtree
    {-# INLINEABLE rfoldli'_ #-}
    rfoldli'_ rec = \f b0 (FoldWithoutKey g) -> case g of
        Leaf vec -> Vec.foldl' (\acc (_k,v) -> acc >>= flip f v) (return b0) vec -- 
        Node vec -> Foldable.foldlM go b0 vec
            where
            go  acc (_set,subtree) = rec f acc subtree

instance FoldFixI (FoldWithKey vec set) where
    {-# INLINEABLE rfoldri_ #-}
    rfoldri_ rec = \f b0 (FoldWithKey g) -> case g of
        Leaf vec -> Vec.foldr (\kv acc -> f kv =<< acc) (return b0) vec -- 
        Node vec -> Foldable.foldrM go b0 vec
            where
            go (_set,subtree) acc = rec f acc subtree
    {-# INLINEABLE rfoldli'_ #-}
    rfoldli'_ rec = \f b0 (FoldWithKey g) -> case g of
        Leaf vec -> Vec.foldl' (\acc kv -> acc >>= flip f kv) (return b0) vec -- 
        Node vec -> Foldable.foldlM go b0 vec
            where
            go  acc (_set,subtree) = rec f acc subtree

data GiSTr vec set key value n rec where
    Leaf :: vec (key,value) -> GiSTr vec set key value 'Z rec
    Node :: V.Vector (set, rec) -> GiSTr vec set key value ('S n) rec

instance Functor (GiSTr vec set k v n) where
    fmap _ (Leaf v) = Leaf v
    fmap f (Node v) = Node $ fmap (\(s,r) -> (s,f r)) v 


-- newtype ComposeI (f :: * -> *) (g :: Nat -> * -> *) (n :: Nat) (a :: *)  = ComposeI {getComposeI :: f (g n a)}

data GiSTn f vec set key value n = GiSTn (FixN n (ComposeI f (GiSTr vec set key value)))


instance (Show set, Show rec, Show (vec (key, value))) => Show (GiSTr vec set key value n rec) where
    show (Leaf vec) = "(Leaf " ++ show vec ++ ")"
    show (Node vec) = "(Node " ++ show vec ++ ")"

instance (Lift Show f, forall n' . Lift Show (g n'), Show a) => Show (ComposeI f g n a) where
    show (ComposeI x) = show x

instance (Show set, Show (vec (key, value)), Lift Show f) => Show (GiSTn f vec set key value n) where
    show (GiSTn x) = show x




data FillFactor = FillFactor {minFill :: Int, maxFill :: Int}

class Ord (Penalty set) => Key key set | set -> key where
    type Penalty set :: *
    exactly :: key -> set
    overlaps :: set -> set -> Bool
    unions :: Foldable f => f set -> set
    penalty :: set -> set -> Penalty set
    partitionSets :: Vector vec (set,val) => FillFactor -> vec (set,val) -> Int -> V2 (set,val) -> AFew (vec (set,val)) 
    -- NB: This can be as simple as "cons" if we don't care about e.g. keeping order among keys
    insertKey :: Vector vec (key,val) => proxy set -> FillFactor -> key -> val -> vec (key,val) -> AFew (vec (key,val)) 



class (Vector vec (key,value), Key key set) => IsGiST vec set key value 
instance (U.Unbox key, U.Unbox value, Key key set) => IsGiST U.Vector set key value


-- GiSTn f vec set key value n

-- data FixN n f where
--     FixZ :: f  'Z     Void      -> FixN  'Z     f
--     FixN :: f ('S n) (FixN n f) -> FixN ('S n) f

preds :: (IsGiST vec set key value, Functor f) => GiSTn f vec set key value n -> f [set]
preds (GiSTn fix) = case fix of 
    FixZ (ComposeI fl) -> fmap (\(Leaf vec) -> map (exactly . fst) $ Vec.toList vec) fl
    FixN (ComposeI fn) -> fmap (\(Node vec) -> map fst $ Vec.toList vec) fn



search' :: forall vec height f set key value m 
        . (IsGiST vec set key value, Monoid m, Monad f) 
        => (vec (key,value) -> f m) -- You can either use the monoid m or the monad f to get the result out.
        -> set 
        -> GiSTn f vec set key value height
        -> f m
search' embed predicate = go
    where
    go :: forall height' . GiSTn f vec set key value height' -> f m
    go (GiSTn fix) = case fix of
        FixZ (ComposeI g) -> do
            q <- g
            case q of
                Leaf vec -> embed (Vec.filter (overlaps predicate . exactly . fst) vec)
        FixN (ComposeI g) -> do
            q <- g
            case q of
                Node vec -> do
                    let f acc (subpred, subgist) = if overlaps predicate subpred 
                        then do
                            subacc <- go (GiSTn subgist)
                            return (acc <> subacc)
                        else return acc
                    Vec.foldM f mempty (vec)




newtype Cat a = Cat ([a] -> [a]) 
    deriving Semigroup via Endo [a]
    deriving Monoid via Endo [a]

cat :: a -> Cat a
cat a = Cat (a:)

dog :: Cat a -> [a]
dog (Cat f) = f []

-- Todo: Implement streaming search by lifting f to a Stream
list' :: (IsGiST vec set key value, Monad f) => set -> GiSTn f vec set key value height -> f (vec (key,value))
list' predicate gist = Vec.concat . dog <$> search' (return . cat) predicate gist


-- -- contains set == not . null . search set
-- -- To actually optimize this properly, I need to provide a primitive like
-- -- m -> f m -> f m, which can short-circuit evaluation of the latter.
-- contains :: IsGist vn vl set key value => set -> GiST vn vl n set key value -> Bool
-- contains predicate = getAny . search' (const (Any True)) predicate


-- f must be included to do the actual work of serializing an f via w
class RW m r w f | m -> r w where
    save :: f (Sum r w x) -> m (w (f x))

instance Traversable f => RW Identity Identity Identity f where
    save f = Identity $ flip traverse f $ \case
                InL x -> x
                InR x -> x



-- insert' :: forall write height read vec set key value .  (Monad read, Functor write, RW read write vec set key value)  
--        => FillFactor -> key -> value 
--        -> GiSTn height read vec set key value -> read (write (Either (GiSTn height write vec set key value) (GiSTn ('S height) write vec set key value)))
-- insert' ff k v (GiSTn g) = insertAndSplit @write ff k v g >>= \case
--             One (_set, gist) -> return $ fmap (Left  . GiSTn) $ gist
--             Two a b         -> return $ fmap (Right . GiSTn) $ saveNode $ Node $ V.fromList [fmap Left a, fmap Left b]
        
insertAndSplit :: forall m r w vec set k v h x
               . (Monad m, (forall h' . RW m r w (GiSTr vec set k v h'))) 
               => FillFactor
               -> k -> v
               ->             GiSTr vec set k v h (r x)     -- The thing to save
               -> m (AFew (w (GiSTr vec set k v h (w x)))) -- That which has been saved
insertAndSplit = undefined
-- insertAndSplit ff@FillFactor{..} key value free  = case free of
--         Pure vec -> return $ fmap (\leaf -> (unions (preds (GiSTn (Pure leaf))), saveLeaf leaf)) $ insertKey (Proxy @set) ff key value vec 
--         node@(Free (Compose (Node vec))) -> 
--             case chooseSubtree (GiSTn node) (exactly key) of
--                 Nothing -> error "GiST node is empty, violating data structure invariants" -- Could also just deal with it
--                 Just (bestIx, best) -> do 
--                     best' <- best
--                     inserted <- insertAndSplit ff key value best'
--                     case inserted of
--                         One (set,gist) -> 
--                             let vec' = Vec.imap (\i old -> if i == bestIx then (set, Left gist) else fmap Right old) vec in
--                             let set' = unions $ map fst $ Vec.toList vec' in 
--                             return (One (set', saveNode (Node vec')))
--                         Two l r -> 
--                             let vec' = fmap (fmap Right) vec in
--                             let wrap vec'' = (unions (fmap fst vec''), saveNode (Node vec'')) in
--                             case partitionSets ff vec' bestIx (V2 (fmap Left l) (fmap Left r)) of 
--                                 One vec'' -> return (One (wrap vec''))
--                                 Two v1 v2 -> return (Two (wrap v1) (wrap v2))



-- class (IsGiST vec set key value) => RW read write vec set key value | write -> read where
--     saveLeaf :: vec (key, value) -> write (Rec 'Z           write vec set key value)
--     saveNode :: Node set (Either   (write (Rec height       write vec set key value)) 
--                                    (read  (Rec height       read  vec set key value))) 
--                                  -> write (Rec ('S height)  write vec set key value) -- Save node

-- instance IsGiST vec set key value => RW Identity Identity vec set key value where
--     saveLeaf = Identity . Pure
--     saveNode = Identity . Free . Compose . fmap (either id id)




-- data Ignoring a o = Ignoring {ignored :: a, unignored :: o}
-- instance Eq o => Eq (Ignoring a o) where x == y = unignored x == unignored y
-- instance Ord o => Ord (Ignoring a o) where compare x y = compare (unignored x) (unignored y)


-- chooseSubtree :: forall vec set key value height f . (Functor f, IsGiST vec set key value)
--               => GiSTn ('S height) f vec set key value 
--               -> set 
--               -> Maybe (Int, f (Rec height f vec set key value))
-- chooseSubtree (GiSTn (Free (Compose (Node vec)))) predicate = ignored . getMin <$> bestSubtree
--     where
--     bestSubtree = Vec.ifoldl (\best i next -> best <> f i next) Nothing vec
--     f ix (subpred, subgist) = Just $ Min $ Ignoring (ix, subgist) (penalty predicate subpred) -- Can replace penalty with any ord


-- data Within a = Within {withinLo :: a, withinHi :: a} | Empty deriving (Eq, Ord, Show)
-- instance Ord a => Semigroup (Within a) where
--     (Within l1 h1) <> (Within l2 h2) = Within (min l1 l2) (max h1 h2)
--     Empty <> a = a
--     a <> Empty = a
-- instance Ord a => Monoid (Within a) where
--     mempty = Empty

-- class Sized f where
--     size :: Num a => f a -> a

-- instance Sized Within where
--     size (Within l h) = h - l
--     size Empty = 0

-- instance (Ord a, Num a) => Key a (Within a) where
--     type Penalty (Within a) = a
--     exactly a = Within a a
--     overlaps (Within l1 h1) (Within l2 h2) = l1 <= h2 && l2 <= h1
--     overlaps Empty _ = False
--     overlaps _ Empty = False
--     unions = fold
--     penalty old new = size (old <> new) - size old
--     insertKey _ ff k v vec = 
--         if Vec.length new <= maxFill ff
--             then One new
--             else let (l,r) = Vec.splitAt (Vec.length new `div` 2) new in Two l r
--         where
--         (before,after) = Vec.span ((<= k) . fst) vec
--         new = Vec.concat [before, Vec.singleton (k,v), after]
--     partitionSets ff vec i (V2 l r) =
--         let (before, after') = Vec.splitAt i vec in
--         let after = Vec.tail after' in
--         let new = Vec.concat [before, Vec.fromList [l,r], after] in
--         if Vec.length new <= maxFill ff
--             then One new
--             else let (lo,hi) = Vec.splitAt (Vec.length new `div` 2) new in Two lo hi
            



data GiST f vec set key value where
    GiST :: GiSTn f vec set key value height -> GiST f vec set key value

instance (Lift Show f, Functor f, Show set, Show (vec (key, value))) => Show (GiST f vec set key value) where
    show (GiST g) = show g

foldrM :: (Monad m, Vector vec (key,v), MonadTrans t, (forall n . Monad n => Monad (t n))) => (v -> b -> t m b) -> b -> GiST m vec set key v -> t m b
foldrM f b (GiST (GiSTn g)) = rfoldri f b $ hoistNL (swozzle FoldWithoutKey) g


foldriM :: (Monad m, Vector vec (k,v), MonadTrans t, (forall n . Monad n => Monad (t n))) => ((k,v) -> b -> t m b) -> b -> GiST m vec set k v -> t m b
foldriM f b (GiST (GiSTn g)) = rfoldri f b $ hoistNL (swozzle FoldWithKey) g


foldrvM :: (Monad m, Vector vec (k,v), MonadTrans t, (forall n . Monad n => Monad (t n))) => (vec (k,v) -> b -> t m b) -> b -> GiST m vec set k v -> t m b
foldrvM f b (GiST (GiSTn g)) = rfoldri f b $ hoistNL (swozzle FoldWithVec) g



foldlM' :: (Monad m, Vector vec (key,v), MonadTrans t, (forall n . Monad n => Monad (t n))) => (b -> v -> t m b) -> b -> GiST m vec set key v -> t m b
foldlM' f b (GiST (GiSTn g)) = rfoldli' f b $ hoistNL (swozzle FoldWithoutKey) g

foldliM' :: (Monad m, Vector vec (k,v), MonadTrans t, (forall n . Monad n => Monad (t n))) => (b -> (k,v) -> t m b) -> b -> GiST m vec set k v -> t m b
foldliM' f b (GiST (GiSTn g)) = rfoldli' f b $ hoistNL (swozzle FoldWithKey) g

foldlvM' :: (Monad m, Vector vec (k,v), MonadTrans t, (forall n . Monad n => Monad (t n))) => (b -> vec (k,v) -> t m b) -> b -> GiST m vec set k v -> t m b
foldlvM' f b (GiST (GiSTn g)) = rfoldli' f b $ hoistNL (swozzle FoldWithVec) g



cronk :: Monad m => (a -> b -> c) -> (a -> b -> m c)
cronk f a b = return (f a b)

foldr :: (Monad m, Vector vec (key,v)) => (v -> b -> b) -> b -> GiST m vec set key v -> m b
foldr f b = runIdentityT . foldrM (cronk f) b 

foldri :: (Monad m, Vector vec (k,v)) => ((k,v) -> b -> b) -> b -> GiST m vec set k v -> m b
foldri f b = runIdentityT . foldriM (cronk f) b 

foldrv :: (Monad m, Vector vec (k,v)) => (vec (k,v) -> b -> b) -> b -> GiST m vec set k v -> m b
foldrv f b = runIdentityT . foldrvM (cronk f) b 


foldl' :: (Monad m, Vector vec (key,v)) => (b -> v -> b) -> b -> GiST m vec set key v -> m b
foldl' f b = runIdentityT . foldlM' (cronk f) b 

foldli' :: (Monad m, Vector vec (k,v)) => (b -> (k,v) -> b) -> b -> GiST m vec set k v -> m b
foldli' f b = runIdentityT . foldliM' (cronk f) b

foldlv' :: (Monad m, Vector vec (k,v)) => (b -> vec (k,v) -> b) -> b -> GiST m vec set k v -> m b
foldlv' f b = runIdentityT . foldlvM' (cronk f) b

-- sum :: (Num a, Monad f, Vector vec (key, a)) => GiST f vec set key a -> f a
-- sum  = runIdentityT . foldl' (\a b -> return (a + b)) 0


-- insert :: (Monad read, Functor write, RW read write vec set key value)  
--        => FillFactor -> key -> value 
--        -> GiST read vec set key value -> read (write (GiST write vec set key value))
-- insert ff k v (GiST g) = fmap (fmap (either GiST GiST)) $ insert' ff k v g

-- empty :: IsGiST vec set key value => GiST f vec set key value
-- empty = GiST (GiSTn (Pure Vec.empty))

-- list :: (Monad f, IsGiST vec set key value) => set -> GiST f vec set key value -> f (vec (key,value))
-- list set (GiST g) = list' set g



