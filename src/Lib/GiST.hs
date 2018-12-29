module Lib.GiST where

-- import GHC.TypeLits (Nat, type (+))
import Data.Vector.Generic as Vec (Vector, concat, filter, foldl, cons, toList, imap, ifoldl, singleton, length, fromList, head, tail, splitAt, empty)
import Data.Monoid (Endo(..), Any(Any,getAny))
import Data.Semigroup (Min(Min,getMin))
import Data.Foldable (fold)
import Data.List (sort, sortOn, splitAt)
import Control.Monad.Free (Free(Free,Pure))
import Data.Functor.Compose (Compose(Compose))

data Nat = Z | S Nat

data AFew xs = One xs | Two xs xs deriving Functor


-- class (forall m . Vector vn (RecursiveEntry vn vl ('S m) p a), Vector vl a, Vector vl a, GiSTy p a) => GiSToid vn vl p a

class Ord (Penalty set) => Key key set | set -> key where
    type Penalty set :: *
    exactly :: key -> set
    overlaps :: set -> set -> Bool
    unions :: Foldable f => f set -> set
    penalty :: set -> set -> Penalty set

class Partition set where
    partition :: (Key key set, GiSTy vn vl set key value) => FillFactor -> GiST vn vl height set key value -> AFew (GiST vn vl height set key value)


class (forall height . Vector vn (Rec vn vl ('S height) set key value), Vector vl (key, value), Key key set) => GiSTy vn vl set key value 


data GiST vn vl (height :: Nat) set key value where
    Leaf :: vl (key, value) 
            -> GiST vn vl 'Z          set key value
    Node ::     Rec vn vl ('S height) set key value 
         -> vn (Rec vn vl ('S height) set key value) 
         ->    GiST vn vl ('S height) set key value

-- Have to make this because of 
-- "Data constructor ‘NodeEntry’ cannot be used here
--        (it is defined and used in the same recursive group)"
data Rec vn vl height set key value where
    Rec :: set 
        -> GiST vn vl     height  set key value 
        -> Rec  vn vl ('S height) set key value


free :: GiSTy vn vl set key value => GiST vn vl height set key value -> Free (Compose [] ((,) set)) [(key, value)]
free (Leaf as) = Pure (Vec.toList as)
free (Node e es) = Free $ Compose $ map (\(Rec p g) -> (p, free g)) $ e : Vec.toList es


instance (GiSTy vn vl set key value, Show value, Show set, Show key) => Show (GiST vn vl height set key value) where
    show = show . free

preds :: GiSTy vn vl set key value => GiST vn vl height set key value -> [set]
preds (Leaf vec) = map (exactly . fst) $ Vec.toList vec
preds (Node e es) = map (\(Rec p _) -> p) $ e : Vec.toList es




data Entry vn vl height set key value where
    LeafEntry :: key -> value -> Entry vn vl 'Z set key value
    NodeEntry :: Rec vn vl ('S height) set key value -> Entry vn vl ('S height) set key value




search' :: forall vn vl height set key value m 
        . (GiSTy vn vl set key value, Monoid m) 
        => (vl (key,value) -> m) 
        -> set 
        -> GiST vn vl height set key value 
        -> m
search' embed predicate =  go
    where
    go :: forall height' . GiST vn vl height' set key value -> m
    go gist = case gist of
        Leaf vec -> embed (Vec.filter (overlaps predicate . exactly . fst) vec)
        Node e vec -> 
            let f acc (Rec subpred subgist) = if overlaps predicate subpred then acc <> go subgist else acc
            in 
            Vec.foldl f (f mempty e) vec


newtype Cat a = Cat ([a] -> [a]) 
    deriving Semigroup via Endo [a]
    deriving Monoid via Endo [a]

cat :: a -> Cat a
cat a = Cat (a:)

dog :: Cat a -> [a]
dog (Cat f) = f []

search :: GiSTy vn vl set key value => set -> GiST vn vl n set key value -> vl (key,value)
search predicate  = Vec.concat . dog . search' cat predicate

-- contains set == not . null . search set
contains :: GiSTy vn vl set key value => set -> GiST vn vl n set key value -> Bool
contains predicate = getAny . search' (const (Any True)) predicate

data FillFactor = FillFactor {minFill :: Int, maxFill :: Int}

insert :: (GiSTy vn vl set key value, Partition set) => FillFactor -> key -> value 
       -> GiST vn vl height set key value -> Either (GiST vn vl height set key value) (GiST vn vl ('S height) set key value)
insert ff k v g = case insertAndSplit ff  k v g of
            One gist -> Left gist
            Two g1 g2 -> Right $ Node (wrap g1) (Vec.singleton (wrap g2))
        

insertAndSplit :: forall vn vl height set key value . (GiSTy vn vl set key value, Partition set) 
               => FillFactor 
               -> key -> value
               ->       GiST vn vl height set key value
               -> AFew (GiST vn vl height set key value) 
insertAndSplit  ff@FillFactor{..} key value node  = case node of
        Leaf vec -> partition ff (Leaf $ Vec.cons (key,value) vec) -- NB: The leaf partition function should keep order if desired, need to figure out an API for that
        Node e vec -> partition ff (Node eNew vecNew)
            where 
            (bestIx, best) = chooseSubtree node (exactly key)
            inserted = insertAndSplit ff key value best 
            (eNew, vecNew) = case inserted of
                One gist -> case bestIx of
                    Zero -> (wrap gist, vec)
                    OnePlus ix -> (e, Vec.imap (\i old -> if i == ix then wrap gist else old) vec)
                Two new1 new2 -> case bestIx of
                    Zero -> (wrap new1, Vec.cons (wrap new2) vec)
                    OnePlus ix -> (e, Vec.cons (wrap new1) (Vec.imap (\i old -> if i == ix then (wrap new2) else old) vec))

wrap :: GiSTy vn vl set key value => GiST vn vl height set key value-> Rec vn vl ('S height) set key value
wrap node = Rec (unions $ preds node) node

data Ignoring a o = Ignoring {ignored :: a, unignored :: o}
instance Eq o => Eq (Ignoring a o) where x == y = unignored x == unignored y
instance Ord o => Ord (Ignoring a o) where compare x y = compare (unignored x) (unignored y)

data Index = Zero | OnePlus Int

chooseSubtree :: GiSTy vn vl set key value 
              => GiST vn vl ('S height) set key value 
              -> set 
              -> (Index, GiST vn vl height set key value)
chooseSubtree (Node e vec) predicate = ignored $ getMin $ Vec.ifoldl (\best i next -> best <> f (OnePlus i) next) (f Zero e) vec
    where
    f ix (Rec subpred subgist) = Min $ Ignoring (ix, subgist) (penalty predicate subpred) -- Can replace penalty with any ord


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


instance (Ord a) => Partition (Within a) where
    partition ff (Leaf v) = if Vec.length v <= maxFill ff
        then One (Leaf v)
        else Two (Leaf l) (Leaf r)
        where (l,r) = Vec.splitAt (Vec.length v `div` 2) v
    partition ff (Node e v) = if Vec.length v + 1 <= maxFill ff
        then One (Node e v)
        else Two (Node el vl) (Node er vr)
        where 
        el = e
        (vl,vr') = Vec.splitAt (Vec.length v `div` 2) v
        (er,vr) = (Vec.head vr', Vec.tail vr')




data SomeGiST vn vl set key value where
    SomeGiST :: GiST vn vl height set key value -> SomeGiST vn vl set key value

instance (GiSTy vn vl set key value, Show value, Show key, Show set) => Show (SomeGiST vn vl set key value) where
    show (SomeGiST g) = show g

insert' :: (GiSTy vn vl set key value, Partition set) => FillFactor -> key -> value -> SomeGiST vn vl set key value -> SomeGiST vn vl set key value
insert' ff k v (SomeGiST g) = either SomeGiST SomeGiST $ insert ff k v g

empty :: GiSTy vn vl set key value => SomeGiST vn vl set key value
empty = SomeGiST (Leaf Vec.empty)

search'' ::GiSTy vn vl set key value => set -> SomeGiST vn vl set key value -> vl (key,value)
search'' set (SomeGiST g) = search set g

