module Lib.GiST where

-- import GHC.TypeLits (Nat, type (+))
import Data.Vector.Generic as Vec (Vector, concat, filter, foldl, cons, toList, imap, ifoldl, singleton, length, fromList)
import Data.Monoid (Endo(..), Any(Any,getAny))
import Data.Semigroup (Min(Min,getMin))
import Data.Foldable (fold)
import Data.List (sort, sortOn, splitAt)
import Control.Monad.Free (Free(Free,Pure))
import Data.Functor.Compose (Compose(Compose))

data Nat = Z | S Nat

data Two xs = Two xs xs deriving Functor

-- class (forall m . Vector vn (RecursiveEntry vn vl ('S m) p a), Vector vl a, Vector vl a, GiSTy p a) => GiSToid vn vl p a

class Predicate pred where
    type Specificity pred :: *
    type Precise pred :: *
    precisely :: Precise pred -> pred
    unions :: Foldable f => f pred -> pred
    specificity :: pred -> pred -> Specificity pred

class (Vector vl (Precise pred, value)) => GiSTy vn vl pred value 


data GiST vn vl (height :: Nat) pred value where
    Leaf :: vl (Precise pred, value) 
            -> GiST vn vl 'Z pred value
    Node ::     Rec vn vl ('S height) pred value 
         -> vn (Rec vn vl ('S height) pred value) 
         ->    GiST vn vl ('S height) pred value

-- Have to make this because of 
-- "Data constructor ‘NodeEntry’ cannot be used here
--        (it is defined and used in the same recursive group)"
data Rec vn vl height pred value where
    Rec :: pred 
        -> GiST vn vl height pred value 
        -> Rec vn vl ('S height) pred value


-- free :: GiSToid vn vl p a => GiST vn vl n p a -> Free (Compose [] ((,) (p a))) [a]
-- free (Leaf as) = Pure (Vec.toList as)
-- free (Node e es) = Free $ Compose $ map (\(RecursiveEntry p g) -> (p, free g)) $ e : Vec.toList es


-- instance (GiSToid vn vl p a, Show a, Show (p a)) => Show (GiST vn vl n p a) where
--     show = show . free

-- preds :: GiSToid vn vl p a => GiST vn vl n p a -> [p a]
-- preds (Leaf vec) = map exactly $ Vec.toList vec
-- preds (Node e es) = map (\(RecursiveEntry p _) -> p) $ e : Vec.toList es




-- data Entry vn vl n p a where
--     LeafEntry :: a -> Entry vn vl 'Z p a
--     NodeEntry :: RecursiveEntry vn vl ('S n) p a -> Entry vn vl ('S n) p a 

-- newtype Penalty = Penalty Int deriving (Eq,Ord)

-- class GiSTy p a where
--     exactly :: a -> p a
--     -- Not completely disjoint
--     consistent :: p a -> p a -> Bool
--     union :: Foldable f => f (p a) -> p a
--     penalty :: p a -> p a -> Penalty
--     pickSplit :: GiSToid vn vl p a => FillFactor -> GiST vn vl n p a -> Either (GiST vn vl n p a ) (Two (GiST vn vl n p a)) 





-- search' :: forall vn vl n p a m . (GiSToid vn vl p a, Monoid m) => (vl a -> m) -> p a -> GiST vn vl n p a -> m
-- search' embed predicate =  go
--     where
--     go :: forall n' . GiST vn vl n' p a -> m
--     go gist = case gist of
--         Leaf vec -> embed (Vec.filter (consistent predicate . exactly) vec)
--         Node e vec -> 
--             let f acc (RecursiveEntry subpred subgist) = if consistent predicate subpred then acc <> go subgist else acc
--             in 
--             Vec.foldl f (f mempty e) vec


-- newtype Cat a = Cat ([a] -> [a]) 
--     deriving Semigroup via Endo [a]
--     deriving Monoid via Endo [a]

-- cat :: a -> Cat a
-- cat a = Cat (a:)

-- dog :: Cat a -> [a]
-- dog (Cat f) = f []

-- search :: GiSToid vn vl p a => p a -> GiST vn vl n p a -> vl a
-- search predicate  = Vec.concat . dog . search' cat predicate

-- contains :: GiSToid vn vl p a => p a -> GiST vn vl n p a -> Bool
-- contains predicate = getAny . search' (const (Any True)) predicate

-- data FillFactor = FillFactor {minFill :: Int, maxFill :: Int}

-- insert :: (GiSToid vn vl p a) => FillFactor -> a -> GiST vn vl n p a -> Either (GiST vn vl n p a) (GiST vn vl ('S n) p a)
-- insert ff i g = case insertAndSplit ff i g of
--             Left gist -> Left gist
--             Right (Two g1 g2) -> Right $ Node (wrap g1) (Vec.singleton (wrap g2))
        

-- insertAndSplit :: forall vn vl p a n . (GiSToid vn vl p a) 
--                => FillFactor 
--                -> a
--                -> GiST vn vl n p a 
--                -> Either (GiST vn vl n p a) 
--                     (Two (GiST vn vl n p a))
-- insertAndSplit  ff@FillFactor{..} insertion node  = pickSplit ff $ updated
--     where 
--     updated = case node of
--         Leaf vec -> Leaf $ Vec.cons insertion vec
--         Node e vec -> Node eNew vecNew
--             where 
--             (bestIx, best) = chooseSubtree node (exactly insertion)
--             inserted = insertAndSplit ff insertion best 
--             (eNew, vecNew) = case inserted of
--                 Left gist -> case bestIx of
--                     Nothing -> (wrap gist, vec)
--                     Just ix -> (e, Vec.imap (\i old -> if i == ix then wrap gist else old) vec)
--                 Right (Two new1 new2) -> case bestIx of
--                     Nothing -> (wrap new1, Vec.cons (wrap new2) vec)
--                     Just ix -> (e, Vec.cons (wrap new1) (Vec.imap (\i old -> if i == ix then (wrap new2) else old) vec))

-- wrap :: GiSToid vn vl p a => GiST vn vl n p a -> RecursiveEntry vn vl ('S n) p a
-- wrap node = RecursiveEntry (union $ preds node) node

-- data Ignoring a o = Ignoring {ignored :: a, unignored :: o}
-- instance Eq o => Eq (Ignoring a o) where x == y = unignored x == unignored y
-- instance Ord o => Ord (Ignoring a o) where compare x y = compare (unignored x) (unignored y)

-- chooseSubtree :: (GiSToid vn vl p a) => GiST vn vl ('S n) p a -> p a  -> (Maybe Int, GiST vn vl n p a)
-- chooseSubtree (Node e vec) predicate = ignored $ getMin $ Vec.ifoldl (\best i next -> best <> f (Just i) next) (f Nothing e) vec
--     where
--     f ix (RecursiveEntry subpred subgist) = Min $ Ignoring (ix, subgist) (penalty predicate subpred) -- Can replace penalty with any ord


-- data Within a = Within {withinLo :: a, withinHi :: a} | Empty deriving (Eq, Ord, Show)
-- instance Ord a => Semigroup (Within a) where
--     (Within l1 h1) <> (Within l2 h2) = Within (min l1 l2) (max h1 h2)
--     Empty <> a = a
--     a <> Empty = a
-- instance Ord a => Monoid (Within a) where
--     mempty = Empty

-- range :: Distant a => Within a -> Int
-- range (Within h l) = (h <-> l) + 1
-- range Empty = 0

-- class Distant a where
--     (<->) :: a -> a -> Int

-- instance Distant Int where
--     a <-> b = abs (a - b)

-- instance (Distant a, Ord a) => GiSTy Within a where
--     exactly a = Within a a
--     consistent (Within l1 h1) (Within l2 h2) = l1 <= h2 && h1 >= l2
--     consistent _ Empty = False
--     consistent Empty _ = False
--     union = fold 
--     penalty new old = Penalty $ abs (range (new <> old) - range old)
--     pickSplit FillFactor{..} gist = case gist of
--         Leaf vec -> if Vec.length vec <= maxFill 
--                         then Left (Leaf vec)
--                         else let (l,r) = splitAt (maxFill `div` 2) (sort $ Vec.toList vec) in 
--                              Right $ Two (Leaf $ Vec.fromList l) (Leaf $ Vec.fromList r)
--         Node e es -> if Vec.length es + 1 <= maxFill
--                         then Left (Node e es)
--                         else let (l:ls,r:rs) = splitAt (maxFill `div` 2) 
--                                     (sortOn (\(RecursiveEntry p _) -> p) $ e : Vec.toList es)  in
--                              Right $ Two (Node l $ Vec.fromList ls) (Node r $ Vec.fromList rs)



-- data SomeGiST vn vl p a where
--     SomeGiST :: GiST vn vl n p a -> SomeGiST vn vl p a

-- instance (GiSToid vn vl p a, Show a, Show (p a)) => Show (SomeGiST vn vl p a) where
--     show (SomeGiST g) = show g

-- insert' :: GiSToid vn vl p a => FillFactor -> a -> SomeGiST vn vl p a -> SomeGiST vn vl p a
-- insert' ff a (SomeGiST g) = either SomeGiST SomeGiST $ insert ff a g

