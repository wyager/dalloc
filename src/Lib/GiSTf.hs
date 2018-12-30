module Lib.GiSTf where

-- import GHC.TypeLits (Nat, type (+))
import qualified Data.Vector as V (Vector)
import Data.Vector.Generic as Vec (Vector, concat, filter, foldl, cons, toList, imap, ifoldl, singleton, length, fromList, head, tail, splitAt, empty, foldM)
import Data.Monoid (Endo(..), Any(Any,getAny))
import Data.Semigroup (Min(Min,getMin))
import Data.Foldable (fold)
import Data.List (sort, sortOn, splitAt)
-- import Control.Monad.Free (Free(Free,Pure))
import Data.Functor.Compose (Compose(Compose))
import GHC.Exts (Constraint)
import Data.Proxy (Proxy(Proxy))

data Nat = Z | S Nat

data AFew xs = One xs | Two xs xs deriving Functor



class Ord (Penalty set) => Key key set | set -> key where
    type Penalty set :: *
    exactly :: key -> set
    overlaps :: set -> set -> Bool
    unions :: Foldable f => f set -> set
    penalty :: set -> set -> Penalty set
    partitionSets :: Vector vec (set,val) => FillFactor -> vec (set,val) -> AFew (vec (set,val)) -- Insertion spot is handled by "penalty"
    -- Insertion is not handled by "penalty", must go here
    -- NB: This can be as simple as "cons" if we don't care about e.g. keeping order among keys
    insertKey :: Vector vec (key,val) => proxy set -> FillFactor -> key -> val -> vec (key,val) -> AFew (vec (key,val)) 


data Freen (n :: Nat) f a where
    Pure :: a -> Freen 'Z f a
    Free :: f (Freen n f a) -> Freen ('S n) f a

instance Functor f => Functor (Freen n f) where
    fmap f (Pure a) = Pure (f a)
    fmap f (Free g) = Free (fmap (fmap f) g)


type Hoist c f = (forall s . c s => c (f s) :: Constraint)

instance (Hoist Show f, Show a) => Show (Freen height f a) where
    show (Pure a) = "Pure " ++ show a
    show (Free f) = "Free " ++ show f

newtype Node set a = Node (V.Vector (set,a)) deriving Functor

newtype GiSTn height f vec set key value = GiSTn (Freen height (Compose (Node set) f) (vec (key,value)))


class (Vector vec (key,value), Key key set) => IsGiST vec set key value 



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




-- class Partition x where
--     partition :: Vector vec (x,val) => FillFactor -> vec (x,val) -> AFew (vec (x,val))

-- class (forall height . Vector vn (Rec vn vl ('S height) set key value), Vector vl (key, value), Key key set) => GiSTy vn vl set key value 



-- -- Have to make this because of 
-- -- "Data constructor ‘NodeEntry’ cannot be used here
-- --        (it is defined and used in the same recursive group)"
-- data Rec vn vl height set key value where
--     Rec :: set 
--         -> GiST vn vl     height  set key value 
--         -> Rec  vn vl ('S height) set key value


-- data Entry vn vl height set key value where
--     LeafEntry :: key -> value -> Entry vn vl 'Z set key value
--     NodeEntry :: Rec vn vl ('S height) set key value -> Entry vn vl ('S height) set key value






newtype Cat a = Cat ([a] -> [a]) 
    deriving Semigroup via Endo [a]
    deriving Monoid via Endo [a]

cat :: a -> Cat a
cat a = Cat (a:)

dog :: Cat a -> [a]
dog (Cat f) = f []

-- Todo: Implement streaming search by lifting f to a Stream
search :: (IsGiST vec set key value, Monad f) => set -> GiSTn height f vec set key value -> f (vec (key,value))
search predicate gist = Vec.concat . dog <$> search' (return . cat) predicate gist


-- contains set == not . null . search set
-- To actually optimize this properly, I need to provide a primitive like
-- m -> f m -> f m, which can short-circuit evaluation of the latter.
-- contains :: GiSTy vn vl set key value => set -> GiST vn vl n set key value -> Bool
-- contains predicate = getAny . search' (const (Any True)) predicate

data FillFactor = FillFactor {minFill :: Int, maxFill :: Int}

-- insert :: (GiSTy vn vl set key value, Partition set) => FillFactor -> key -> value 
--        -> GiST vn vl height set key value -> Either (GiST vn vl height set key value) (GiST vn vl ('S height) set key value)
-- insert ff k v g = case insertAndSplit ff  k v g of
--             One gist -> Left gist
--             Two g1 g2 -> Right $ Node (wrap g1) (Vec.singleton (wrap g2))
        

-- insertAndSplit :: forall vn vl height set key value . (GiSTy vn vl set key value, Partition set) 
--                => FillFactor 
--                -> key -> value
--                ->       GiST vn vl height set key value
--                -> AFew (GiST vn vl height set key value) 
-- insertAndSplit  ff@FillFactor{..} key value node  = case node of
--         Leaf vec -> partition ff (Leaf $ Vec.cons (key,value) vec) -- NB: The leaf partition function should keep order if desired, need to figure out an API for that
--         Node e vec -> partition ff (Node eNew vecNew)
--             where 
--             (bestIx, best) = chooseSubtree node (exactly key)
--             inserted = insertAndSplit ff key value best 
--             (eNew, vecNew) = case inserted of
--                 One gist -> case bestIx of
--                     Zero -> (wrap gist, vec)
--                     OnePlus ix -> (e, Vec.imap (\i old -> if i == ix then wrap gist else old) vec)
--                 Two new1 new2 -> case bestIx of
--                     Zero -> (wrap new1, Vec.cons (wrap new2) vec)
--                     OnePlus ix -> (e, Vec.cons (wrap new1) (Vec.imap (\i old -> if i == ix then (wrap new2) else old) vec))

-- wrap :: GiSTy vn vl set key value => GiST vn vl height set key value-> Rec vn vl ('S height) set key value
-- wrap node = Rec (unions $ preds node) node

-- data Ignoring a o = Ignoring {ignored :: a, unignored :: o}
-- instance Eq o => Eq (Ignoring a o) where x == y = unignored x == unignored y
-- instance Ord o => Ord (Ignoring a o) where compare x y = compare (unignored x) (unignored y)

-- data Index = Zero | OnePlus Int

-- chooseSubtree :: GiSTy vn vl set key value 
--               => GiST vn vl ('S height) set key value 
--               -> set 
--               -> (Index, GiST vn vl height set key value)
-- chooseSubtree (Node e vec) predicate = ignored $ getMin $ Vec.ifoldl (\best i next -> best <> f (OnePlus i) next) (f Zero e) vec
--     where
--     f ix (Rec subpred subgist) = Min $ Ignoring (ix, subgist) (penalty predicate subpred) -- Can replace penalty with any ord


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


-- instance (Ord a) => Partition (Within a) where
--     partition ff (Leaf v) = if Vec.length v <= maxFill ff
--         then One (Leaf v)
--         else Two (Leaf l) (Leaf r)
--         where (l,r) = Vec.splitAt (Vec.length v `div` 2) v
--     partition ff (Node e v) = if Vec.length v + 1 <= maxFill ff
--         then One (Node e v)
--         else Two (Node el vl) (Node er vr)
--         where 
--         el = e
--         (vl,vr') = Vec.splitAt (Vec.length v `div` 2) v
--         (er,vr) = (Vec.head vr', Vec.tail vr')




-- data SomeGiST vn vl set key value where
--     SomeGiST :: GiST vn vl height set key value -> SomeGiST vn vl set key value

-- instance (GiSTy vn vl set key value, Show value, Show key, Show set) => Show (SomeGiST vn vl set key value) where
--     show (SomeGiST g) = show g

-- insert' :: (GiSTy vn vl set key value, Partition set) => FillFactor -> key -> value -> SomeGiST vn vl set key value -> SomeGiST vn vl set key value
-- insert' ff k v (SomeGiST g) = either SomeGiST SomeGiST $ insert ff k v g

-- empty :: GiSTy vn vl set key value => SomeGiST vn vl set key value
-- empty = SomeGiST (Leaf Vec.empty)

-- search'' ::GiSTy vn vl set key value => set -> SomeGiST vn vl set key value -> vl (key,value)
-- search'' set (SomeGiST g) = search set g

