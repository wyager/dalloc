module Lib.GiSTf_ex where

import qualified Lib.GiSTf2 as G
import           Data.Vector.Unboxed (Vector)
import           Data.Functor.Identity (Identity, runIdentity)
import qualified Streaming.Prelude as SP


e :: G.GiST Identity Vector (G.Within Int) Int Char
e = runIdentity $ G.empty


f :: G.GiST Identity Vector (G.Within Int) Int Char
f = foldl (\g (k,v) -> runIdentity $ G.insert (G.FillFactor 4 8) k v g) e (zip [1..] (['A'..'Z'] ++ ['a'..'z']))


s :: SP.Stream (SP.Of Char) Identity ()
s = G.foldlM' id (const SP.yield) () f

