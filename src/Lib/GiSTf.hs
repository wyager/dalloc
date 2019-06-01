module Lib.GiSTf where

-- import GHC.TypeLits (Nat, type (+))
import qualified Data.Vector as V (Vector, fromList)
import Data.Vector.Generic as Vec (Vector, concat, filter, toList, imap, ifoldl, singleton, length, fromList, tail, splitAt, empty, foldM, span)
import Data.Monoid (Endo(..))
import Data.Semigroup (Min(Min,getMin))
import Data.Foldable (fold)
-- import Control.Monad.Free (Free(Free,Pure))
import Data.Functor.Compose (Compose(Compose))
import GHC.Exts (Constraint)
import Data.Proxy (Proxy(Proxy))
import Data.Functor.Identity (Identity(..))
import qualified Data.Vector.Unboxed as U
-- import qualified Lib.Schemes as S

data Nat = Z | S Nat

data AFew xs = One xs | Two xs xs deriving (Functor, Foldable, Traversable)

data V2 x = V2 x x

type f ∘ g = Compose f g

class Ord (Penalty set) => Key key set | set -> key where
    type Penalty set :: *
    exactly :: key -> set
    overlaps :: set -> set -> Bool
    unions :: Foldable f => f set -> set
    penalty :: set -> set -> Penalty set
    partitionSets :: Vector vec (set,val) => FillFactor -> vec (set,val) -> Int -> V2 (set,val) -> AFew (vec (set,val)) 
    -- NB: This can be as simple as "cons" if we don't care about e.g. keeping order among keys
    insertKey :: Vector vec (key,val) => proxy set -> FillFactor -> key -> val -> vec (key,val) -> AFew (vec (key,val)) 


data Freen (n :: Nat) f a where
    Pure :: a -> Freen 'Z f a
    Free :: f (Freen n f a) -> Freen ('S n) f a


iterM :: (Monad m, Functor f) => (f (m a) -> m a) -> Freen n f a -> m a
iterM _ (Pure a) = return a
iterM g (Free f) = g (fmap (iterM g) f)

instance Functor f => Functor (Freen n f) where
    fmap f (Pure a) = Pure (f a)
    fmap f (Free g) = Free (fmap (fmap f) g)

type Lift c f = (forall s . c s => c (f s) :: Constraint)

instance (Lift Show f, Show a) => Show (Freen height f a) where
    show (Pure a) = "Pure " ++ show a
    show (Free f) = "Free " ++ show f

newtype Node set a = Node (V.Vector (set,a)) deriving (Functor, Show)



type Rec height f vec set key value = Freen height (Node set ∘ f) (vec (key,value))
newtype GiSTn height f vec set key value = GiSTn (Rec height f vec set key value)

instance (Lift Show f, Functor f, Show set, Show (vec (key, value))) => Show (GiSTn height f vec set key value) where
    show (GiSTn gist) = case gist of
        Pure a -> "Leaf " ++ show a
        Free (Compose (Node vec)) -> "Node " ++ show (fmap (\(set,node) -> (set,fmap GiSTn node)) vec)


class (Vector vec (key,value), Key key set) => IsGiST vec set key value 
instance (U.Unbox key, U.Unbox value, Key key set) => IsGiST U.Vector set key value



preds :: IsGiST vec set key value => GiSTn height f vec set key value -> [set]
preds (GiSTn free) = case free of 
    Pure vec -> map (exactly . fst) $ Vec.toList vec
    Free (Compose (Node vec)) -> map fst $ Vec.toList vec



search' :: forall vec height f set key value m 
        . (IsGiST vec set key value, Monoid m, Monad f) 
        => (vec (key,value) -> f m) -- You can either use the monoid m or the monad f to get the result out.
        -> set 
        -> GiSTn height f vec set key value 
        -> f m
search' embed predicate = go
    where
    go :: forall height' . GiSTn height' f vec set key value -> f m
    go (GiSTn free) = case free of
        Pure vec -> embed (Vec.filter (overlaps predicate . exactly . fst) vec)
        Free (Compose (Node vec)) -> 
            let f acc (subpred, subgist) = if overlaps predicate subpred 
                then do
                    free' <- subgist
                    subacc <- go (GiSTn free')
                    return (acc <> subacc)
                else return acc
            in 
            Vec.foldM f mempty vec




newtype Cat a = Cat ([a] -> [a]) 
    deriving Semigroup via Endo [a]
    deriving Monoid via Endo [a]

cat :: a -> Cat a
cat a = Cat (a:)

dog :: Cat a -> [a]
dog (Cat f) = f []

-- Todo: Implement streaming search by lifting f to a Stream
list' :: (IsGiST vec set key value, Monad f) => set -> GiSTn height f vec set key value -> f (vec (key,value))
list' predicate gist = Vec.concat . dog <$> search' (return . cat) predicate gist


-- contains set == not . null . search set
-- To actually optimize this properly, I need to provide a primitive like
-- m -> f m -> f m, which can short-circuit evaluation of the latter.
-- contains :: GiSTy vn vl set key value => set -> GiST vn vl n set key value -> Bool
-- contains predicate = getAny . search' (const (Any True)) predicate

data FillFactor = FillFactor {minFill :: Int, maxFill :: Int}



class (IsGiST vec set key value) => RW read write vec set key value | write -> read where
    saveLeaf :: vec (key, value) -> write (Rec 'Z           write vec set key value)
    saveNode :: Node set (Either   (write (Rec height       write vec set key value)) 
                                   (read  (Rec height       read  vec set key value))) 
                                 -> write (Rec ('S height)  write vec set key value) -- Save node

instance IsGiST vec set key value => RW Identity Identity vec set key value where
    saveLeaf = Identity . Pure
    saveNode = Identity . Free . Compose . fmap (either id id)

insert' :: forall write height read vec set key value .  (Monad read, Functor write, RW read write vec set key value)  
       => FillFactor -> key -> value 
       -> GiSTn height read vec set key value -> read (write (Either (GiSTn height write vec set key value) (GiSTn ('S height) write vec set key value)))
insert' ff k v (GiSTn g) = insertAndSplit @write ff k v g >>= \case
            One (_set, gist) -> return $ fmap (Left  . GiSTn) $ gist
            Two a b         -> return $ fmap (Right . GiSTn) $ saveNode $ Node $ V.fromList [fmap Left a, fmap Left b]
        
insertAndSplit :: forall write height read vec set key value . (Monad read, RW read write vec set key value) 
               => FillFactor
               -> key -> value
               ->                         Rec height read vec set key value     -- The thing to save
               -> read (AFew (set, write (Rec height write vec set key value))) -- That which has been saved
insertAndSplit ff@FillFactor{..} key value free  = case free of
        Pure vec -> return $ fmap (\leaf -> (unions (preds (GiSTn (Pure leaf))), saveLeaf leaf)) $ insertKey (Proxy @set) ff key value vec 
        node@(Free (Compose (Node vec))) -> 
            case chooseSubtree (GiSTn node) (exactly key) of
                Nothing -> error "GiST node is empty, violating data structure invariants" -- Could also just deal with it
                Just (bestIx, best) -> do 
                    best' <- best
                    inserted <- insertAndSplit ff key value best'
                    case inserted of
                        One (set,gist) -> 
                            let vec' = Vec.imap (\i old -> if i == bestIx then (set, Left gist) else fmap Right old) vec in
                            let set' = unions $ map fst $ Vec.toList vec' in 
                            return (One (set', saveNode (Node vec')))
                        Two l r -> 
                            let vec' = fmap (fmap Right) vec in
                            let wrap vec'' = (unions (fmap fst vec''), saveNode (Node vec'')) in
                            case partitionSets ff vec' bestIx (V2 (fmap Left l) (fmap Left r)) of 
                                One vec'' -> return (One (wrap vec''))
                                Two v1 v2 -> return (Two (wrap v1) (wrap v2))
                    -- return (fmap save inserted)


data Ignoring a o = Ignoring {ignored :: a, unignored :: o}
instance Eq o => Eq (Ignoring a o) where x == y = unignored x == unignored y
instance Ord o => Ord (Ignoring a o) where compare x y = compare (unignored x) (unignored y)


chooseSubtree :: forall vec set key value height f . (Functor f, IsGiST vec set key value)
              => GiSTn ('S height) f vec set key value 
              -> set 
              -> Maybe (Int, f (Rec height f vec set key value))
chooseSubtree (GiSTn (Free (Compose (Node vec)))) predicate = ignored . getMin <$> bestSubtree
    where
    bestSubtree = Vec.ifoldl (\best i next -> best <> f i next) Nothing vec
    f ix (subpred, subgist) = Just $ Min $ Ignoring (ix, subgist) (penalty predicate subpred) -- Can replace penalty with any ord


data Within a = Within {withinLo :: a, withinHi :: a} | Empty deriving (Eq, Ord, Show)
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
    GiST :: GiSTn height f vec set key value -> GiST f vec set key value

instance (Lift Show f, Functor f, Show set, Show (vec (key, value))) => Show (GiST f vec set key value) where
    show (GiST g) = show g


insert :: (Monad read, Functor write, RW read write vec set key value)  
       => FillFactor -> key -> value 
       -> GiST read vec set key value -> read (write (GiST write vec set key value))
insert ff k v (GiST g) = fmap (fmap (either GiST GiST)) $ insert' ff k v g

empty :: IsGiST vec set key value => GiST f vec set key value
empty = GiST (GiSTn (Pure Vec.empty))

list :: (Monad f, IsGiST vec set key value) => set -> GiST f vec set key value -> f (vec (key,value))
list set (GiST g) = list' set g



