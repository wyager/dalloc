module Lib.GiSTf_ex where

import           Lib.GiSTf as G
import           Data.Vector.Unboxed (Vector)
import           Data.Functor.Identity (Identity, runIdentity)



e :: GiST Identity Vector (Within Int) Int Char
e = G.empty


f :: GiST Identity Vector (Within Int) Int Char
f = foldl (\g (k,v) -> runIdentity $ runIdentity $ insert (FillFactor 4 8) k v g) e (take 20 $ zip [1..] ['a'..])