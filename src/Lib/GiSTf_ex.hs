module Lib.GiSTf_ex where

import qualified Lib.GiSTf2 as G
import           Data.Vector.Unboxed (Vector)
import           Data.Vector.Generic (singleton)
import           Data.Functor.Identity (Identity, runIdentity)
import qualified Streaming.Prelude as SP
import qualified Data.Map.Strict as Map
import           Control.Monad (foldM)



e :: G.GiST Identity Vector (G.Within Int) Int Char
e = runIdentity $ G.empty


f :: G.GiST Identity Vector (G.Within Int) Int Char
f = foldl (\g (k,v) -> runIdentity $ G.insert (G.FillFactor 4 8) k v g) e (zip [1..] (['A'..'Z'] ++ ['a'..'z']))


s :: SP.Stream (SP.Of Char) Identity ()
s = G.foldlM' id (const SP.yield) () f


testMapEquivalence :: forall vec set k v m rw proxy . (G.IsGiST vec set k v, G.BackingStore m rw rw vec set k v, Monad m, G.R m rw, Eq (vec (k,v)), Ord k, Eq v) => proxy (G.GiST rw vec set k v) -> G.FillFactor -> [(k,v)]-> m Bool
testMapEquivalence _ ff assocs = do
        theGiST <- create 
        (&&) <$> accessEquivalence theGiST <*> foldEquivalence theGiST
    where
    theMap = Map.fromList assocs
    elems = Map.toList theMap -- deduplicated
    create = G.empty >>= \empty -> foldM (\g (k,v) -> G.insert @vec @set ff k v g) empty  elems
    -- create = Map.foldlWithKey (\g k v -> g >>= G.insert ff k v) (return G.empty) theMap
    accessEquivalence g = and <$> mapM (testAccess g) elems
    testAccess g (k,v) = do
        matching <- G.list (G.exactly k) g
        return $ matching == singleton (k,v)
    foldEquivalence gist = do
        gistList <- G.foldr G.read (:) [] gist
        gistListK <- G.foldri G.read (:) [] gist
        return (gistListK == Map.toList theMap && gistList == Map.elems theMap)
