{-# LANGUAGE UndecidableInstances #-} -- Show constraints

{-# OPTIONS_GHC -fno-warn-orphans #-}

module Lib.Structures.GiST (
    GiST, Within(..), FillFactor(..), Transforming(..),
    IsGiST, R, BackingStore, read, exactly,
    empty, insert, search,
    foldrM, foldriM, foldrvM, foldlM', foldliM', foldlvM', foldr, foldri, foldrv, foldl', foldli', foldlv',
    mergeOn
) where

import Prelude hiding (read, foldr)
import qualified Data.Vector as V (Vector, fromList)
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Storable as VS
import Data.Vector.Generic as Vec (Vector, toList, filter, foldM, concat, ifoldl', length, splitAt, singleton, tail, fromList, (//)) 
import qualified Data.Vector.Generic as Vec 
import qualified Data.Vector.Generic.Mutable as VecM
import Data.Semigroup (Min(Min,getMin))
import qualified Data.Foldable as Foldable (foldrM, foldlM)
import Data.Proxy (Proxy(Proxy))
import Data.Functor.Identity (Identity(..))
import Lib.Structures.Schemes (Nat(S,Z), FixN(FixZ,FixN),ComposeI(..),FoldFixI,Lift, rfoldri_, rfoldli'_, rfoldli', rfoldri)
import Control.Monad.Trans.Identity (runIdentityT)
import Control.Monad.Trans.Class (MonadTrans)
import Data.Void (Void)
import Data.Foldable (fold)
import Control.Monad.Trans.Class (lift)

import Control.DeepSeq (NFData, rnf)
import GHC.Generics (Generic)

import Data.Strict.Tuple (Pair((:!:)))
import qualified Data.Strict.Tuple as T2

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
            go (_set :!: subtree) acc = rec f acc subtree
    {-# INLINEABLE rfoldli'_ #-}
    rfoldli'_ rec = \f b0 (FoldWithoutKey g) -> case g of
        Leaf vec -> Vec.foldl' (\acc (_k,v) -> acc >>= flip f v) (return b0) vec -- 
        Node vec -> Foldable.foldlM go b0 vec
            where
            go  acc (_set :!: subtree) = rec f acc subtree

instance FoldFixI (FoldWithKey vec set) where
    {-# INLINEABLE rfoldri_ #-}
    rfoldri_ rec = \f b0 (FoldWithKey g) -> case g of
        Leaf vec -> Vec.foldr (\kv acc -> f kv =<< acc) (return b0) vec -- 
        Node vec -> Foldable.foldrM go b0 vec
            where
            go (_set :!: subtree) acc = rec f acc subtree
    {-# INLINEABLE rfoldli'_ #-}
    rfoldli'_ rec = \f b0 (FoldWithKey g) -> case g of
        Leaf vec -> Vec.foldl' (\acc kv -> acc >>= flip f kv) (return b0) vec -- 
        Node vec -> Foldable.foldlM go b0 vec
            where
            go  acc (_set :!: subtree) = rec f acc subtree


instance FoldFixI (FoldWithVec set) where
    {-# INLINEABLE rfoldri_ #-}
    rfoldri_ rec = \f b0 (FoldWithVec g) -> case g of
        Leaf vec -> f vec b0
        Node vec -> Foldable.foldrM go b0 vec
            where
            go (_set :!: subtree) acc = rec f acc subtree
    {-# INLINEABLE rfoldli'_ #-}
    rfoldli'_ rec = \f b0 (FoldWithVec g) -> case g of
        Leaf vec -> f b0 vec 
        Node vec -> Foldable.foldlM go b0 vec
            where
            go  acc (_set :!: subtree) = rec f acc subtree

data GiSTr vec set key value n rec where
    Leaf :: !(vec (key,value)) -> GiSTr vec set key value 'Z rec
    Node :: !(V.Vector (Pair set rec)) -> GiSTr vec set key value ('S n) rec

instance (NFData a, NFData b) => NFData (Pair a b) where
    rnf (a :!: b) = rnf (a,b)
instance Functor (Pair a) where 
    fmap f (a :!: b) = (a :!: f b )


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
    partitionSets :: Vector vec (Pair set val) => FillFactor -> vec (Pair set val) -> Int -> V2 (Pair set val) -> AFew (vec (Pair set val)) 
    -- NB: This can be as simple as "cons" if we don't care about e.g. keeping order among keys
    insertKey :: Vector vec (key,val) => proxy set -> FillFactor -> key -> val -> vec (key,val) -> AFew (vec (key,val)) 

class (Vector vec (key,value), Key key set) => IsGiST vec set key value 
instance (VU.Unbox key, VU.Unbox value, Key key set) => IsGiST VU.Vector set key value
instance (VS.Storable (key,value), Key key set) => IsGiST VS.Vector set key value
instance Key key set => IsGiST V.Vector set key value

{-# INLINE search' #-}
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
                    let f acc (subpred :!: subgist) = if overlaps predicate subpred 
                        then do
                            subacc <- go (GiSTn subgist)
                            return (acc <> subacc)
                        else return acc
                    Vec.foldM f mempty (vec)

{-# INLINABLE search #-}
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
    {-# INLINE saveS #-}
    saveS = Identity . Identity
    {-# INLINE saveZ #-}
    saveZ = Identity . Identity
    {-# INLINE leaveS #-}
    leaveS _ = id


{-# INLINABLE insert' #-}
insert' :: forall m r w vec set k v h .  (Monad m, R m r, IsGiST vec set k v, BackingStore m r w vec set k v)  
       =>
       FillFactor -> k -> v 
       -> GiSTn r vec set k v h
       -> m (Either ((GiSTn w vec set k v h)) ((GiSTn w vec set k v ('S h))))
insert' ff k v g= insertAndSplit @m @r @w ff k v g >>= \case
            One (_set :!: gist) -> return $ Left gist
            Two (aSet  :!:  GiSTn aGiSTn) (bSet :!: GiSTn bGiSTn) -> fmap (Right . GiSTn . FixN . ComposeI) $ saveS node 
                where
                node =  Node $ V.fromList [(aSet :!: aGiSTn), (bSet :!: bGiSTn)]

{-# INLINE insertAndSplit #-}
insertAndSplit :: forall m r w vec set k v h
               . (Monad m, R m r, IsGiST vec set k v, BackingStore m r w vec set k v) 
               => FillFactor
               -> k -> v
               ->          GiSTn r vec set k v h    
               -> m (AFew (Pair set (GiSTn w vec set k v h))) -- That which has been saved
insertAndSplit ff@FillFactor{..} key value = go 
    where
    go :: forall h' . GiSTn r vec set k v h' -> m (AFew (Pair set (GiSTn w vec set k v h')))
    go !(GiSTn (FixZ (ComposeI (free))))  = do
        vec <- read @m @r free >>= \case Leaf vec -> return vec
        let newVecs :: AFew (vec (k, v))
            newVecs = insertKey (Proxy @set) ff key value vec  
            setOf = unions . map (exactly . fst) . Vec.toList 
            save = fmap (GiSTn . FixZ . ComposeI) . saveZ . Leaf
        traverse (\entries -> ((setOf entries :: set)  :!:) <$> save entries) newVecs    
    go !(GiSTn (FixN (ComposeI (free)))) = do
        node <- read @m @r free 
        let vec = foo $ case node of Node v -> foo v 
        case chooseSubtree node (exactly key) of
            Nothing -> error "GiST node is empty, violating data structure invariants"
            Just (bestIx, best) -> do
                inserted <- go (GiSTn best)
                let reuse r = let (GiSTn w) = leaveS (Proxy @m) (GiSTn r) in w
                    force v =  Vec.foldl' (\u p -> p `seq` u) () v
                case inserted of
                    One (set  :!: GiSTn gist) -> do
                        let vec' = fmap (fmap reuse) vec Vec.// [(bestIx,(set :!: gist))]
                        let set' = unions $ map T2.fst $ Vec.toList vec'
                        let !() = force vec'
                        saved <- saveS (Node vec')
                        return $ One (set' :!: (GiSTn $ FixN $ ComposeI saved))
                    Two (setL :!: GiSTn l) (setR  :!:  GiSTn r) -> do
                        let untouched = fmap (fmap reuse) vec
                        let wrap vec' = do
                                let !() = force vec'
                                saved <- saveS (Node vec')
                                return (unions (fmap T2.fst vec') :!: (GiSTn $ FixN $ ComposeI saved))
                        case partitionSets ff untouched bestIx (V2 (setL :!: l) (setR :!: r)) of
                            One v -> One <$> wrap v
                            Two v1 v2 -> Two <$> wrap v1 <*> wrap v2


foo :: V.Vector (Pair a b) -> V.Vector (Pair a b)
foo = id

{-# INLINE chooseSubtree #-}
chooseSubtree :: forall vec set k v h x . (IsGiST vec set k v)
              => GiSTr vec set k v ('S h) x
              -> set 
              -> Maybe (Int, x)
chooseSubtree (Node vec) predicate = ignored . getMin <$> bestSubtree
    where
    bestSubtree = Vec.ifoldl' (\best i next -> best <> f i next) Nothing vec
    f ix (subpred  :!:  subgist) = Just $ Min $ Ignoring (ix, subgist) (penalty predicate subpred) -- Can replace penalty with any ord


data Ignoring a o = Ignoring {ignored :: a, unignored :: o}
instance Eq o => Eq (Ignoring a o) where x == y = unignored x == unignored y
instance Ord o => Ord (Ignoring a o) where compare x y = compare (unignored x) (unignored y)


data Within a = Within {withinLo :: !a, withinHi :: !a} | Empty 
    deriving (Eq, Ord, Show, Generic)
    deriving anyclass NFData

instance Ord a => Semigroup (Within a) where
    {-# INLINE (<>) #-}
    (Within l1 h1) <> (Within l2 h2) = Within (min l1 l2) (max h1 h2)
    Empty <> a = a
    a <> Empty = a
instance Ord a => Monoid (Within a) where
    mempty = Empty

class Sized f where
    size :: Num a => f a -> a

instance Sized Within where
    {-# INLINE size #-}
    size (Within l h) = h - l
    size Empty = 0

instance (Ord a, Num a) => Key a (Within a) where
    type Penalty (Within a) = a
    {-# INLINE exactly #-}
    exactly a = Within a a
    {-# INLINE overlaps #-}
    overlaps (Within l1 h1) (Within l2 h2) = l1 <= h2 && l2 <= h1
    overlaps Empty _ = False
    overlaps _ Empty = False
    {-# INLINE unions #-}
    unions = fold
    {-# INLINE penalty #-}
    penalty old new = size (old <> new) - size old
    {-# INLINE insertKey #-}
    insertKey _ ff k v vec = 
        if Vec.length new <= maxFill ff
            then One new
            else let (l,r) = Vec.splitAt (Vec.length new `div` 2) new in Two l r
        where
        new = mergeOn fst vec (Vec.singleton (k,v))
    {-# INLINE partitionSets #-}
    partitionSets ff vec i (V2 l r) =
        let (before, after') = Vec.splitAt i vec in
        let after = Vec.tail after' in
        let new = Vec.concat [before, Vec.fromList [l,r], after] in
        if Vec.length new <= maxFill ff
            then One new
            else let (lo,hi) = Vec.splitAt (Vec.length new `div` 2) new in Two lo hi
            

-- Merge two already sorted vectors
{-# INLINE mergeOn #-}
mergeOn :: (Ord b, Vector v a) => (a -> b) -> v a -> v a -> v a
mergeOn f a b = Vec.create $ do
    let la = Vec.length a
    let lb = Vec.length b
    let lv = la + lb
    v <- VecM.new lv
    let go !ia !ib
            | ia == la && ib == lb = return ()
            | ib == lb = do
                let av =  a Vec.! ia
                VecM.write v (ia + ib) av >> go (ia+1) ib
            | ia == la = do
                let bv =  b Vec.! ib
                VecM.write v (ia + ib) bv >> go ia (ib+1)
            | otherwise = do
                let av =  a Vec.! ia
                let bv =  b Vec.! ib
                if f av < f bv
                    then VecM.write v (ia + ib) av >> go (ia+1) ib
                    else VecM.write v (ia + ib) bv >> go ia (ib+1)
    go 0 0
    return v


data GiST f vec set key value where
    GiST :: !(GiSTn f vec set key value height) -> GiST f vec set key value

instance (NFData (vec (key,value)), NFData set, forall a . NFData a => NFData (f a)) => NFData (GiST f vec set key value) where
    rnf (GiST g) = rnf g

instance (Lift Show f, Functor f, Show set, Show (vec (key, value))) => Show (GiST f vec set key value) where
    show (GiST g) = show g

{-# INLINE cronk #-}
cronk :: Monad m => (a -> b -> c) -> (a -> b -> m c)
cronk f a b = return (f a b)


{-# INLINE foldrM #-}
foldrM :: (Monad m, Vector vec (key,v), MonadTrans t, (forall n . Monad n => Monad (t n))) =>  (forall x . r x -> m x) -> (v -> b -> t m b) -> b -> GiST r vec set key v -> t m b
foldrM r2m f b (GiST (GiSTn g)) = rfoldri r2m FoldWithoutKey f b g


{-# INLINE foldriM #-}
foldriM :: (Monad m, Vector vec (k,v), MonadTrans t, (forall n . Monad n => Monad (t n))) =>  (forall x . r x -> m x) -> ((k,v) -> b -> t m b) -> b -> GiST r vec set k v -> t m b
foldriM r2m f b (GiST (GiSTn g)) = rfoldri r2m FoldWithKey f b g


{-# INLINE foldrvM #-}
foldrvM :: (Monad m, Vector vec (k,v), MonadTrans t, (forall n . Monad n => Monad (t n))) =>  (forall x . r x -> m x) -> (vec (k,v) -> b -> t m b) -> b -> GiST r vec set k v -> t m b
foldrvM r2m f b (GiST (GiSTn g)) = rfoldri r2m FoldWithVec f b g

{-# INLINE foldlM' #-}
foldlM' :: (Monad m, Vector vec (key,v), MonadTrans t, (forall n . Monad n => Monad (t n))) =>  (forall x . r x -> m x) -> (b -> v -> t m b) -> b -> GiST r vec set key v -> t m b
foldlM' r2m f b (GiST (GiSTn g)) = rfoldli' r2m FoldWithoutKey f b g

{-# INLINE foldliM' #-}
foldliM' :: (Monad m, Vector vec (k,v), MonadTrans t, (forall n . Monad n => Monad (t n))) =>  (forall x . r x -> m x) -> (b -> (k,v) -> t m b) -> b -> GiST r vec set k v -> t m b
foldliM' r2m f b (GiST (GiSTn g)) = rfoldli' r2m FoldWithKey f b g

{-# INLINE foldlvM' #-}
foldlvM' :: (Monad m, Vector vec (k,v), MonadTrans t, (forall n . Monad n => Monad (t n))) =>  (forall x . r x -> m x) -> (b -> vec (k,v) -> t m b) -> b -> GiST r vec set k v -> t m b
foldlvM' r2m f b (GiST (GiSTn g)) = rfoldli' r2m FoldWithVec f b g


{-# INLINE foldr #-}
foldr :: (Monad m, Vector vec (key,v)) => (forall x . r x -> m x) -> (v -> b -> b) -> b -> GiST r vec set key v -> m b
foldr r2m f b = runIdentityT . foldrM r2m (cronk f) b 

{-# INLINE foldri #-}
foldri :: (Monad m, Vector vec (k,v)) => (forall x . r x -> m x) -> ((k,v) -> b -> b) -> b -> GiST r vec set k v -> m b
foldri r2m f b = runIdentityT . foldriM r2m (cronk f) b 

{-# INLINE foldrv #-}
foldrv :: (Monad m, Vector vec (k,v)) => (forall x . r x -> m x) -> (vec (k,v) -> b -> b) -> b -> GiST r vec set k v -> m b
foldrv r2m f b = runIdentityT . foldrvM r2m (cronk f) b 

{-# INLINE foldl' #-}
foldl' :: (Monad m, Vector vec (key,v)) => (forall x . r x -> m x) -> (b -> v -> b) -> b -> GiST r vec set key v -> m b
foldl' r2m f b = runIdentityT . foldlM' r2m (cronk f) b 

{-# INLINE foldli' #-}
foldli' :: (Monad m, Vector vec (k,v)) => (forall x . r x -> m x) -> (b -> (k,v) -> b) -> b -> GiST r vec set k v -> m b
foldli' r2m f b = runIdentityT . foldliM' r2m (cronk f) b

{-# INLINE foldlv' #-}
foldlv' :: (Monad m, Vector vec (k,v)) => (forall x . r x -> m x) -> (b -> vec (k,v) -> b) -> b -> GiST r vec set k v -> m b
foldlv' r2m f b = runIdentityT . foldlvM' r2m (cronk f) b


empty :: forall vec set k v m r w . (IsGiST vec set k v, BackingStore m r w vec set k v, Functor m) => m (GiST w vec set k v)
empty = fmap (GiST . GiSTn . FixZ . ComposeI) $ saveZ $ Leaf Vec.empty

{-# INLINE insert #-}
insert :: forall vec set k v m r w .  (IsGiST vec set k v, BackingStore m r w vec set k v, Monad m, R m r) => FillFactor -> k -> v -> GiST r vec set k v -> m (GiST w vec set k v)
insert ff k v (GiST !g) = either GiST GiST <$> (insert' ff k v g)



