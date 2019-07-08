-- Attempting to reconcile depth-typed GiST with Schemes

{-# LANGUAGE UndecidableInstances #-} -- Show constraints
{-# OPTIONS_GHC -fno-warn-orphans #-}

module Lib.GiST (
    GiST, Within(..), FillFactor(..), Transforming(..),
    IsGiST, R, BackingStore, read, exactly,
    empty, insert, search,
    foldrM, foldriM, foldrvM, foldlM', foldliM', foldlvM', foldr, foldri, foldrv, foldl', foldli', foldlv'
) where

import Prelude hiding (read, foldr)
import qualified Data.Vector as V (Vector, fromList)
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Storable as VS
import Data.Vector.Generic as Vec (Vector, toList, filter, foldM, concat, ifoldl', length, splitAt, span, singleton, tail, fromList, (//)) 
import qualified Data.Vector.Generic as Vec (foldr, foldl', empty) 
import Data.Semigroup (Min(Min,getMin))
import qualified Data.Foldable as Foldable (foldrM, foldlM)
import Data.Proxy (Proxy(Proxy))
import Data.Functor.Identity (Identity(..))
import Lib.Schemes (Nat(S,Z), FixN(FixZ,FixN),ComposeI(..),FoldFixI,Lift, rfoldri_, rfoldli'_, rfoldli', rfoldri)
import Control.Monad.Trans.Identity (runIdentityT)
import Control.Monad.Trans.Class (MonadTrans)
import Data.Void (Void)
import Data.Foldable (fold)
import Control.Monad.Trans.Class (lift)

import Control.DeepSeq (NFData, rnf)
import GHC.Generics (Generic)

data AFew xs = One xs | Two xs xs deriving (Functor, Foldable, Traversable)

data V2 x = V2 x x

instance (forall n' . Lift Show (f n')) => Show (FixN n f) where
    show (FixZ f) = show f
    show (FixN f) = show f

data FoldWithoutKey vec set key value n rec where
    FoldWithoutKey :: Vector vec (key,value) => GiSTr vec set key value n rec -> FoldWithoutKey vec set key value n rec

data FoldWithKey vec set kv n rec where
    FoldWithKey :: (Vector vec kv, kv ~ (k,v)) => GiSTr vec set k v n rec -> FoldWithKey vec set kv n rec

data FoldWithVec set vec_kv n rec where
    FoldWithVec :: (Vector vec kv, vec_kv ~ vec kv, kv ~ (k,v)) => GiSTr vec set k v n rec -> FoldWithVec set vec_kv n rec

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

data GiSTr vec set key value n rec where
    Leaf :: !(vec (key,value)) -> GiSTr vec set key value 'Z rec
    Node :: !(V.Vector (set, rec)) -> GiSTr vec set key value ('S n) rec

instance (NFData (vec (key,value)), NFData set, NFData rec) => NFData (GiSTr vec set key value n rec) where
    rnf (Leaf v) = rnf v
    rnf (Node v) = rnf v

data GiSTn f vec set key value n = GiSTn (FixN n (ComposeI f (GiSTr vec set key value)))

instance (forall a . NFData a => NFData (f a), NFData (vec (key,value)), NFData set) => NFData (GiSTn f vec set key value n) where
    rnf (GiSTn g) = rnf g

instance (Show set, Show rec, Show (vec (key, value))) => Show (GiSTr vec set key value n rec) where
    show (Leaf vec) = "(Leaf " ++ show vec ++ ")"
    show (Node vec) = "(Node " ++ show vec ++ ")"

instance (Lift Show f, forall n' . Lift Show (g n'), Show a) => Show (ComposeI f g n a) where
    show (ComposeI x) = show x

instance (Show set, Show (vec (key, value)), Lift Show f) => Show (GiSTn f vec set key value n) where
    show (GiSTn x) = show x

data FillFactor = FillFactor {minFill :: Int, maxFill :: Int} deriving Show

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
instance (VU.Unbox key, VU.Unbox value, Key key set) => IsGiST VU.Vector set key value
instance (VS.Storable (key,value), Key key set) => IsGiST VS.Vector set key value
instance Key key set => IsGiST V.Vector set key value

search' :: forall vec height m set key value q r
        . (IsGiST vec set key value, Monoid q, Monad m, R m r) 
        => (vec (key,value) -> m q) -- You can either use the monoid m or the monad f to get the result out.
        -> set 
        -> GiSTn r vec set key value height
        -> m q
search' embed predicate = go
    where
    go :: forall height' . GiSTn r vec set key value height' -> m q
    go (GiSTn fix) = case fix of
        FixZ (ComposeI g) -> do
            q <- read g
            case q of
                Leaf vec -> embed (Vec.filter (overlaps predicate . exactly . fst) vec)
        FixN (ComposeI g) -> do
            q <- read g
            case q of
                Node vec -> do
                    let f acc (subpred, subgist) = if overlaps predicate subpred 
                        then do
                            subacc <- go (GiSTn subgist)
                            return (acc <> subacc)
                        else return acc
                    Vec.foldM f mempty (vec)

search :: forall vec m set key value q r
        . (IsGiST vec set key value, Monoid q, Monad m, R m r) 
        => (vec (key,value) -> m q) -- You can either use the monoid m or the monad f to get the result out.
        -> set 
        -> GiST r vec set key value
        -> m q 
search e p (GiST g) = search' e p g

class R (m :: * -> *) r | m -> r where
    read :: r x -> m x

instance R Identity Identity where
    read = id

newtype Transforming t n a = Transforming {transformed :: t n a}
    deriving newtype (Functor, Applicative, Monad, MonadTrans)

instance (R m r, Monad m, MonadTrans t) => R (Transforming t m) r where
    read = Transforming . lift . read

type Saver m w x = x -> m (w x)

class BackingStore m r w vec set k v | m -> r w where
    saveS :: forall h . Saver m w (GiSTr vec set k v ('S h) (FixN h (ComposeI w (GiSTr vec set k v))))
    saveZ :: Saver m w (GiSTr vec set k v 'Z Void)
    leaveS :: forall h proxy . proxy m -> GiSTn r vec set k v h -> GiSTn w vec set k v h

instance BackingStore Identity Identity Identity vec set k v where
    saveS = Identity . Identity
    saveZ = Identity . Identity
    leaveS _ = id


-- {-# SPECIALIZE insert' 
{-# INLINABLE insert' #-}
insert' :: forall m r w vec set k v h .  (Monad m, R m r, IsGiST vec set k v, BackingStore m r w vec set k v)  
       =>
       FillFactor -> k -> v 
       -> GiSTn r vec set k v h
       -> m (Either ((GiSTn w vec set k v h)) ((GiSTn w vec set k v ('S h))))
insert' ff k v g= insertAndSplit @m @r @w ff k v g >>= \case
            One (_set, gist) -> return $ Left gist
            Two (aSet, GiSTn aGiSTn) (bSet, GiSTn bGiSTn) -> fmap (Right . GiSTn . FixN . ComposeI) $ saveS node 
                where
                node =  Node $ V.fromList [(aSet, aGiSTn), (bSet, bGiSTn)]

{-# INLINABLE insertAndSplit #-}
insertAndSplit :: forall m r w vec set k v h
               . (Monad m, R m r, IsGiST vec set k v, BackingStore m r w vec set k v) 
               => FillFactor
               -> k -> v
               ->          GiSTn r vec set k v h    
               -> m (AFew (set, (GiSTn w vec set k v h))) -- That which has been saved
insertAndSplit ff@FillFactor{..} key value = go 
    where
    go :: forall h' . GiSTn r vec set k v h' -> m (AFew (set, GiSTn w vec set k v h'))
    go !(GiSTn (FixZ (ComposeI (free))))  = do
        vec <- read @m @r free >>= \case Leaf vec -> return vec
        let newVecs :: AFew (vec (k, v))
            newVecs = insertKey (Proxy @set) ff key value vec  
            setOf = unions . map (exactly . fst) . Vec.toList 
            save = fmap (GiSTn . FixZ . ComposeI) . saveZ . Leaf
        traverse (\entries -> (setOf entries :: set, ) <$> save entries) newVecs    
    go !(GiSTn (FixN (ComposeI (free))))  = do
        node <- read @m @r free 
        let vec = case node of Node v -> v
        case chooseSubtree node (exactly key) of
            Nothing -> error "GiST node is empty, violating data structure invariants"
            Just (bestIx, best) -> do
                inserted <- go (GiSTn best)
                let reuse r = let (GiSTn w) = leaveS (Proxy @m) (GiSTn r) in w
                case inserted of
                    One (set, GiSTn gist) -> do
                        let vec' = fmap (fmap reuse) vec Vec.// [(bestIx,(set,gist))]
                        -- let vec' = Vec.imap (\i old -> if i == bestIx then (set, gist) else fmap reuse old) vec
                        let set' = unions $ map fst $ Vec.toList vec'
                        -- let !_ = Vec.foldl' (flip seq) () vec'
                        saved <- saveS (Node vec')
                        return $ One (set', GiSTn $ FixN $ ComposeI saved)
                    Two (setL, GiSTn l) (setR, GiSTn r) -> do
                        let untouched = fmap (fmap reuse) vec
                        let wrap vec' = do
                                saved <- saveS (Node vec')
                                return (unions (fmap fst vec'),GiSTn $ FixN $ ComposeI saved)
                        case partitionSets ff untouched bestIx (V2 (setL, l) (setR, r)) of
                            One v -> One <$> wrap v
                            Two v1 v2 -> Two <$> wrap v1 <*> wrap v2

{-# INLINE chooseSubtree #-}
chooseSubtree :: forall vec set k v h x . (IsGiST vec set k v)
              => GiSTr vec set k v ('S h) x
              -> set 
              -> Maybe (Int, x)
chooseSubtree (Node vec) predicate = ignored . getMin <$> bestSubtree
    where
    bestSubtree = Vec.ifoldl' (\best i next -> best <> f i next) Nothing vec
    f ix (subpred, subgist) = Just $ Min $ Ignoring (ix, subgist) (penalty predicate subpred) -- Can replace penalty with any ord


data Ignoring a o = Ignoring {ignored :: a, unignored :: o}
instance Eq o => Eq (Ignoring a o) where x == y = unignored x == unignored y
instance Ord o => Ord (Ignoring a o) where compare x y = compare (unignored x) (unignored y)


data Within a = Within {withinLo :: !a, withinHi :: !a} | Empty 
    deriving (Eq, Ord, Show, Generic)
    deriving anyclass NFData

instance Ord a => Semigroup (Within a) where
    (Within l1 h1) <> (Within l2 h2) = Within (min l1 l2) (max h1 h2)
    Empty <> a = a
    a <> Empty = a
instance Ord a => Monoid (Within a) where
    mempty = Empty

class Sized f where
    size :: Num a => f a -> a

instance Sized Within where
    size (Within l h) = h - l
    size Empty = 0

instance (Ord a, Num a) => Key a (Within a) where
    type Penalty (Within a) = a
    exactly a = Within a a
    overlaps (Within l1 h1) (Within l2 h2) = l1 <= h2 && l2 <= h1
    overlaps Empty _ = False
    overlaps _ Empty = False
    unions = fold
    penalty old new = size (old <> new) - size old
    insertKey _ ff k v vec = 
        if Vec.length new <= maxFill ff
            then One new
            else let (l,r) = Vec.splitAt (Vec.length new `div` 2) new in Two l r
        where
        (before,after) = Vec.span ((<= k) . fst) vec
        new = Vec.concat [before, Vec.singleton (k,v), after]
    partitionSets ff vec i (V2 l r) =
        let (before, after') = Vec.splitAt i vec in
        let after = Vec.tail after' in
        let new = Vec.concat [before, Vec.fromList [l,r], after] in
        if Vec.length new <= maxFill ff
            then One new
            else let (lo,hi) = Vec.splitAt (Vec.length new `div` 2) new in Two lo hi
            



data GiST f vec set key value where
    GiST :: !(GiSTn f vec set key value height) -> GiST f vec set key value

instance (NFData (vec (key,value)), NFData set, forall a . NFData a => NFData (f a)) => NFData (GiST f vec set key value) where
    rnf (GiST g) = rnf g

instance (Lift Show f, Functor f, Show set, Show (vec (key, value))) => Show (GiST f vec set key value) where
    show (GiST g) = show g


foldrM :: (Monad m, Vector vec (key,v), MonadTrans t, (forall n . Monad n => Monad (t n))) =>  (forall x . r x -> m x) -> (v -> b -> t m b) -> b -> GiST r vec set key v -> t m b
foldrM r2m f b (GiST (GiSTn g)) = rfoldri r2m FoldWithoutKey f b g


foldriM :: (Monad m, Vector vec (k,v), MonadTrans t, (forall n . Monad n => Monad (t n))) =>  (forall x . r x -> m x) -> ((k,v) -> b -> t m b) -> b -> GiST r vec set k v -> t m b
foldriM r2m f b (GiST (GiSTn g)) = rfoldri r2m FoldWithKey f b g


foldrvM :: (Monad m, Vector vec (k,v), MonadTrans t, (forall n . Monad n => Monad (t n))) =>  (forall x . r x -> m x) -> (vec (k,v) -> b -> t m b) -> b -> GiST r vec set k v -> t m b
foldrvM r2m f b (GiST (GiSTn g)) = rfoldri r2m FoldWithVec f b g



foldlM' :: (Monad m, Vector vec (key,v), MonadTrans t, (forall n . Monad n => Monad (t n))) =>  (forall x . r x -> m x) -> (b -> v -> t m b) -> b -> GiST r vec set key v -> t m b
foldlM' r2m f b (GiST (GiSTn g)) = rfoldli' r2m FoldWithoutKey f b g

foldliM' :: (Monad m, Vector vec (k,v), MonadTrans t, (forall n . Monad n => Monad (t n))) =>  (forall x . r x -> m x) -> (b -> (k,v) -> t m b) -> b -> GiST r vec set k v -> t m b
foldliM' r2m f b (GiST (GiSTn g)) = rfoldli' r2m FoldWithKey f b g

foldlvM' :: (Monad m, Vector vec (k,v), MonadTrans t, (forall n . Monad n => Monad (t n))) =>  (forall x . r x -> m x) -> (b -> vec (k,v) -> t m b) -> b -> GiST r vec set k v -> t m b
foldlvM' r2m f b (GiST (GiSTn g)) = rfoldli' r2m FoldWithVec f b g



cronk :: Monad m => (a -> b -> c) -> (a -> b -> m c)
cronk f a b = return (f a b)

foldr :: (Monad m, Vector vec (key,v)) => (forall x . r x -> m x) -> (v -> b -> b) -> b -> GiST r vec set key v -> m b
foldr r2m f b = runIdentityT . foldrM r2m (cronk f) b 

foldri :: (Monad m, Vector vec (k,v)) => (forall x . r x -> m x) -> ((k,v) -> b -> b) -> b -> GiST r vec set k v -> m b
foldri r2m f b = runIdentityT . foldriM r2m (cronk f) b 

foldrv :: (Monad m, Vector vec (k,v)) => (forall x . r x -> m x) -> (vec (k,v) -> b -> b) -> b -> GiST r vec set k v -> m b
foldrv r2m f b = runIdentityT . foldrvM r2m (cronk f) b 


foldl' :: (Monad m, Vector vec (key,v)) => (forall x . r x -> m x) -> (b -> v -> b) -> b -> GiST r vec set key v -> m b
foldl' r2m f b = runIdentityT . foldlM' r2m (cronk f) b 

foldli' :: (Monad m, Vector vec (k,v)) => (forall x . r x -> m x) -> (b -> (k,v) -> b) -> b -> GiST r vec set k v -> m b
foldli' r2m f b = runIdentityT . foldliM' r2m (cronk f) b

foldlv' :: (Monad m, Vector vec (k,v)) => (forall x . r x -> m x) -> (b -> vec (k,v) -> b) -> b -> GiST r vec set k v -> m b
foldlv' r2m f b = runIdentityT . foldlvM' r2m (cronk f) b


empty :: forall vec set k v m r w . (IsGiST vec set k v, BackingStore m r w vec set k v, Functor m) => m (GiST w vec set k v)
empty = fmap (GiST . GiSTn . FixZ . ComposeI) $ saveZ $ Leaf Vec.empty

{-# SPECIALIZE insert :: FillFactor -> Int -> Int -> GiST Identity VU.Vector (Within Int) Int Int -> Identity (GiST Identity VU.Vector (Within Int) Int Int) #-}
{-# INLINABLE insert #-}
insert :: forall vec set k v m r w .  (IsGiST vec set k v, BackingStore m r w vec set k v, Monad m, R m r) => FillFactor -> k -> v -> GiST r vec set k v -> m (GiST w vec set k v)
insert ff k v (GiST g) = either GiST GiST <$> (insert' ff k v g)



