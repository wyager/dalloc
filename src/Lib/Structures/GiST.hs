{-# LANGUAGE UndecidableInstances #-} -- Show constraints

{-# OPTIONS_GHC -fno-warn-orphans #-}

module Lib.Structures.GiST (
    GiST, saveS, leaveS, Within(..), FillFactor(..), Transforming(..),
    IsGiST, R, BackingStore, Reads, Referent, read, exactly,
    empty, insert, search,
    foldrM, foldriM, foldrvM, foldlM', foldliM', foldlvM', foldr, foldri, foldrv, foldl', foldli', foldlv',
    mergeOn
) where

import qualified Data.Store as S
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
import Data.Functor.Compose (Compose(..))
import Data.Fix (Fix(..))
import Lib.Structures.Schemes (FoldFix, rfoldr_, rfoldl'_, rfoldl', rfoldr)
import Control.Monad.Trans.Identity (runIdentityT)
import Control.Monad.Trans.Class (MonadTrans)
import Data.Foldable (fold)
import Control.Monad.Trans.Class (lift)
import Control.DeepSeq (NFData, rnf)
import GHC.Generics (Generic)
import GHC.Exts (Constraint)

import Data.Strict.Tuple (Pair((:!:)))
import qualified Data.Strict.Tuple as T2
import Data.Functor.Contravariant (contramap)

data AFew xs = One xs | Two xs xs deriving (Functor, Foldable, Traversable)

data V2 x = V2 x x


data FoldWithoutKey vec set key value rec where
    FoldWithoutKey :: Vector vec (key,value) => GiSTr vec set key value rec -> FoldWithoutKey vec set key value rec

data FoldWithKey vec set kv rec where
    FoldWithKey :: (Vector vec kv, kv ~ (k,v)) => GiSTr vec set k v rec -> FoldWithKey vec set kv rec

data FoldWithVec set vec_kv rec where
    FoldWithVec :: (Vector vec kv, vec_kv ~ vec kv, kv ~ (k,v)) => GiSTr vec set k v rec -> FoldWithVec set vec_kv rec

instance FoldFix (FoldWithoutKey vec set key) where
    {-# INLINEABLE rfoldr_ #-}
    rfoldr_ rec = \f b0 (FoldWithoutKey g) -> case g of
        Leaf vec -> Vec.foldr (\(_k,v) acc -> f v =<< acc) (return b0) vec -- 
        Node vec -> Foldable.foldrM go b0 vec
            where
            go (_set :!: subtree) acc = rec f acc subtree
    {-# INLINEABLE rfoldl'_ #-}
    rfoldl'_ rec = \f b0 (FoldWithoutKey g) -> case g of
        Leaf vec -> Vec.foldl' (\acc (_k,v) -> acc >>= flip f v) (return b0) vec -- 
        Node vec -> Foldable.foldlM go b0 vec
            where
            go  acc (_set :!: subtree) = rec f acc subtree

instance FoldFix (FoldWithKey vec set) where
    {-# INLINEABLE rfoldr_ #-}
    rfoldr_ rec = \f b0 (FoldWithKey g) -> case g of
        Leaf vec -> Vec.foldr (\kv acc -> f kv =<< acc) (return b0) vec -- 
        Node vec -> Foldable.foldrM go b0 vec
            where
            go (_set :!: subtree) acc = rec f acc subtree
    {-# INLINEABLE rfoldl'_ #-}
    rfoldl'_ rec = \f b0 (FoldWithKey g) -> case g of
        Leaf vec -> Vec.foldl' (\acc kv -> acc >>= flip f kv) (return b0) vec -- 
        Node vec -> Foldable.foldlM go b0 vec
            where
            go  acc (_set :!: subtree) = rec f acc subtree


instance FoldFix (FoldWithVec set) where
    {-# INLINEABLE rfoldr_ #-}
    rfoldr_ rec = \f b0 (FoldWithVec g) -> case g of
        Leaf vec -> f vec b0
        Node vec -> Foldable.foldrM go b0 vec
            where
            go (_set :!: subtree) acc = rec f acc subtree
    {-# INLINEABLE rfoldl'_ #-}
    rfoldl'_ rec = \f b0 (FoldWithVec g) -> case g of
        Leaf vec -> f b0 vec 
        Node vec -> Foldable.foldlM go b0 vec
            where
            go  acc (_set :!: subtree) = rec f acc subtree

data GiSTr vec set key value rec
    = Leaf !(vec (key,value))
    | Node !(V.Vector (Pair set rec))
    deriving (Generic)

deriving anyclass instance (S.Store (vec (key,value)), S.Store set, S.Store rec) => S.Store (GiSTr vec set key value rec)

deriving anyclass instance S.Store (f (Fix f)) => S.Store (Fix f)
deriving anyclass instance S.Store (f (g a)) => S.Store (Compose f g a)

newtype GiST f vec set key value = GiST (Fix (Compose f (GiSTr vec set key value)))

instance NFData (GiST f vec set key value) where
    rnf (GiST (Fix (Compose f))) = f `seq` ()

instance (S.Store a, S.Store b) => S.Store (Pair a b) where
    size = contramap (\(a :!: b) -> (a,b)) S.size
    peek = fmap (\(a,b) -> (a :!: b)) S.peek
    poke = S.poke . (\(a :!: b) -> (a,b))




instance (NFData a, NFData b) => NFData (Pair a b) where
    rnf (a :!: b) = rnf (a,b)
instance Functor (Pair a) where 
    fmap f (a :!: b) = (a :!: f b )



instance (Show set, Show rec, Show (vec (key, value))) => Show (GiSTr vec set key value rec) where
    show (Leaf vec) = "(Leaf " ++ show vec ++ ")"
    show (Node vec) = "(Node " ++ show vec ++ ")"


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

{-# INLINE search #-}
search :: forall vec m set key value q r
        . (IsGiST vec set key value, Monoid q, Monad m, Reads m r vec set key value) 
        => (vec (key,value) -> m q) -- You can either use the monoid m or the monad f to get the result out.
        -> set 
        -> GiST r vec set key value
        -> m q
search embed predicate = go
    where
    go :: GiST r vec set key value  -> m q
    go (GiST (Fix (Compose g))) = do
            q <- read g
            case q of
                Leaf vec -> embed (Vec.filter (overlaps predicate . exactly . fst) vec)
                Node vec -> do
                    let f acc (subpred :!: subgist) = if overlaps predicate subpred 
                        then do
                            subacc <- go (GiST subgist)
                            return (acc <> subacc)
                        else return acc
                    Vec.foldM f mempty (vec)



class R (m :: * -> *) r | m -> r where
    type Referent m a :: Constraint
    read :: Referent m x => r x -> m x

type Reads m r vec set key value = (R m r, Referent m (GiSTr vec set key value (Fix (Compose r (GiSTr vec set key  value))))) 


instance R Identity Identity where
    type Referent Identity _ = ()
    read = id

newtype Transforming t n a = Transforming {transformed :: t n a}
    deriving newtype (Functor, Applicative, Monad, MonadTrans)

instance (R m r, Monad m, MonadTrans t) => R (Transforming t m) r where
    type Referent (Transforming t m) a = Referent m a
    read = Transforming . lift . read

type Saver m w x = x -> m (w x)

class BackingStore m r w vec set k v | m -> r w where
    saveS :: Saver m w (GiSTr vec set k v (Fix (Compose w (GiSTr vec set k v))))
    leaveS :: forall proxy . proxy m -> GiST r vec set k v -> GiST w vec set k v

instance BackingStore Identity Identity Identity vec set k v where
    {-# INLINE saveS #-}
    saveS = Identity . Identity
    {-# INLINE leaveS #-}
    leaveS _ = id


{-# INLINABLE insert' #-}
insert' :: forall m r w vec set k v .  (Monad m, Reads m r vec set k v, IsGiST vec set k v, BackingStore m r w vec set k v)  
       =>
       FillFactor -> k -> v 
       -> GiST r vec set k v 
       -> m (Either ((GiST w vec set k v)) ((GiST w vec set k v)))
insert' ff k v g= insertAndSplit @m @r @w ff k v g >>= \case
            One (_set :!: gist) -> return $ Left gist
            Two (aSet  :!:  GiST aGiST) (bSet :!: GiST bGiST) -> fmap (Right . GiST . Fix . Compose) $ saveS node 
                where
                node =  Node $ V.fromList [(aSet :!: aGiST), (bSet :!: bGiST)]

{-# INLINE insertAndSplit #-}
insertAndSplit :: forall m r w vec set k v 
               . (Monad m, Reads m r vec set k v, IsGiST vec set k v, BackingStore m r w vec set k v) 
               => FillFactor
               -> k -> v
               ->          GiST r vec set k v  
               -> m (AFew (Pair set (GiST w vec set k v))) -- That which has been saved
insertAndSplit ff@FillFactor{..} key value = go 
    where
    go :: GiST r vec set k v  -> m (AFew (Pair set (GiST w vec set k v)))
    go !(GiST (Fix (Compose (free))))  = do
        read @m @r free >>= \case 
            Leaf vec -> do
                let newVecs :: AFew (vec (k, v))
                    newVecs = insertKey (Proxy @set) ff key value vec  
                    setOf = unions . map (exactly . fst) . Vec.toList 
                    save = fmap (GiST . Fix . Compose) . saveS . Leaf
                traverse (\entries -> ((setOf entries :: set)  :!:) <$> save entries) newVecs    
            Node vec -> do
                case chooseSubtree vec (exactly key) of
                    Nothing -> error "GiST node is empty, violating data structure invariants"
                    Just (bestIx, best) -> do
                        inserted <- go (GiST best)
                        let reuse r = let (GiST w) = leaveS (Proxy @m) (GiST r) in w
                            force v =  Vec.foldl' (\u p -> p `seq` u) () v
                        case inserted of
                            One (set  :!: GiST gist) -> do
                                let vec' = fmap (fmap reuse) vec Vec.// [(bestIx,(set :!: gist))]
                                let set' = unions $ map T2.fst $ Vec.toList vec'
                                let !() = force vec'
                                saved <- saveS (Node vec')
                                return $ One (set' :!: (GiST $ Fix $ Compose saved))
                            Two (setL :!: GiST l) (setR  :!:  GiST r) -> do
                                let untouched = fmap (fmap reuse) vec
                                let wrap vec' = do
                                        let !() = force vec'
                                        saved <- saveS (Node vec')
                                        return (unions (fmap T2.fst vec') :!: (GiST $ Fix $ Compose saved))
                                case partitionSets ff untouched bestIx (V2 (setL :!: l) (setR :!: r)) of
                                    One v -> One <$> wrap v
                                    Two v1 v2 -> Two <$> wrap v1 <*> wrap v2



{-# INLINE chooseSubtree #-}
chooseSubtree :: forall set k x . (Key k set)
              => V.Vector (Pair set x)
              -> set 
              -> Maybe (Int, x)
chooseSubtree  vec predicate = ignored . getMin <$> bestSubtree
    where
    bestSubtree = Vec.ifoldl' (\best i next -> best <> f i next) Nothing vec
    f ix (subpred  :!:  subgist) = Just $ Min $ Ignoring (ix, subgist) (penalty predicate subpred) -- Can replace penalty with any ord


data Ignoring a o = Ignoring {ignored :: a, unignored :: o}
instance Eq o => Eq (Ignoring a o) where x == y = unignored x == unignored y
instance Ord o => Ord (Ignoring a o) where compare x y = compare (unignored x) (unignored y)


data Within a = Within {withinLo :: !a, withinHi :: !a} | Empty 
    deriving (Eq, Ord, Show, Generic)
    deriving anyclass (NFData, S.Store)


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




{-# INLINE cronk #-}
cronk :: Monad m => (a -> b -> c) -> (a -> b -> m c)
cronk f a b = return (f a b) 


{-# INLINE foldrM #-}
foldrM :: (Monad m, Vector vec (key,v), MonadTrans t, (forall n . Monad n => Monad (t n))) =>  (forall x . r x -> m x) -> (v -> b -> t m b) -> b -> GiST r vec set key v -> t m b
foldrM r2m f b (GiST g) = rfoldr r2m FoldWithoutKey f b g


{-# INLINE foldriM #-}
foldriM :: (Monad m, Vector vec (k,v), MonadTrans t, (forall n . Monad n => Monad (t n))) =>  (forall x . r x -> m x) -> ((k,v) -> b -> t m b) -> b -> GiST r vec set k v -> t m b
foldriM r2m f b (GiST g) = rfoldr r2m FoldWithKey f b g


{-# INLINE foldrvM #-}
foldrvM :: (Monad m, Vector vec (k,v), MonadTrans t, (forall n . Monad n => Monad (t n))) =>  (forall x . r x -> m x) -> (vec (k,v) -> b -> t m b) -> b -> GiST r vec set k v -> t m b
foldrvM r2m f b (GiST g) = rfoldr r2m FoldWithVec f b g

{-# INLINE foldlM' #-}
foldlM' :: (Monad m, Vector vec (key,v), MonadTrans t, (forall n . Monad n => Monad (t n))) =>  (forall x . r x -> m x) -> (b -> v -> t m b) -> b -> GiST r vec set key v -> t m b
foldlM' r2m f b (GiST g) = rfoldl' r2m FoldWithoutKey f b g

{-# INLINE foldliM' #-}
foldliM' :: (Monad m, Vector vec (k,v), MonadTrans t, (forall n . Monad n => Monad (t n))) =>  (forall x . r x -> m x) -> (b -> (k,v) -> t m b) -> b -> GiST r vec set k v -> t m b
foldliM' r2m f b (GiST g) = rfoldl' r2m FoldWithKey f b g

{-# INLINE foldlvM' #-}
foldlvM' :: (Monad m, Vector vec (k,v), MonadTrans t, (forall n . Monad n => Monad (t n))) =>  (forall x . r x -> m x) -> (b -> vec (k,v) -> t m b) -> b -> GiST r vec set k v -> t m b
foldlvM' r2m f b (GiST g) = rfoldl' r2m FoldWithVec f b g


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
empty = fmap (GiST . Fix . Compose) $ saveS $ Leaf Vec.empty

{-# INLINE insert #-}
insert :: forall vec set k v m r w .  (IsGiST vec set k v, BackingStore m r w vec set k v, Monad m, Reads m r vec set k v) => FillFactor -> k -> v -> GiST r vec set k v -> m (GiST w vec set k v)
insert ff k v g = either id id <$> (insert' ff k v g)



