module Lib.Structures.GiST.Example where

import qualified Lib.Structures.GiST as G
import Data.Vector.Unboxed (Vector)
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Generic as VG
import Data.Functor.Identity (Identity, runIdentity)
import qualified Streaming.Prelude as SP
-- import           Data.Foldable (foldl')


e :: VU.Unbox v => G.GiST Identity Vector (G.Within Int) Int v
e = runIdentity $ G.empty


f :: G.GiST Identity Vector (G.Within Int) Int Char
f = foldl
  (\g (k, v) -> runIdentity $ G.insert (G.FillFactor 4 8) k v g)
  e
  (zip [1 ..] (['A' .. 'Z'] ++ ['a' .. 'z']))

s :: SP.Stream (SP.Of Char) Identity ()
s = G.foldlM' id (const SP.yield) () f

s2 :: SP.Stream (SP.Of (Int, Char)) Identity ()
s2 = G.transformed
  $ G.search (VU.foldM_ (\() (k, v) -> G.Transforming $ SP.yield (k, v)) ()) (G.Within 6 20) f

{-# INLINE bigSet #-}
bigSet :: G.FillFactor -> Int -> G.GiST Identity Vector (G.Within Int) Int Int
bigSet ff n = runIdentity $ bigSet' ff n

{-# INLINE bigSet' #-}
bigSet' :: forall vec rw m . (G.IsGiST vec (G.Within Int) Int Int, G.BackingStore m rw rw vec (G.Within Int) Int Int, Monad m, G.Reads m rw vec (G.Within Int) Int Int) => G.FillFactor -> Int -> m (G.GiST rw vec (G.Within Int) Int Int)
bigSet' ff n = go 0 =<< G.empty
  where
  go !i !g 
    | i == n = return g
    | otherwise = do
        g' <- G.insert ff i i g
        go (i + 1) g'

force :: G.GiST Identity Vector (G.Within Int) Int Int -> Int
force g = runIdentity $ G.foldli' id (\a (k, v) -> a + k + v) 0 g

speedFold :: G.GiST Identity Vector (G.Within Int) Int Int -> Int
speedFold = runIdentity . G.foldl' id (+) 0

speedFoldV :: G.GiST Identity Vector (G.Within Int) Int Int -> Int
speedFoldV = runIdentity . G.foldlv' id (\acc v -> acc + VU.sum (VU.map snd v)) 0

speedFoldlS :: G.GiST Identity Vector (G.Within Int) Int Int -> Int
speedFoldlS big = SP.fst' $ runIdentity $ SP.sum stream
 where
  stream :: SP.Stream (SP.Of Int) Identity ()
  stream = G.foldlM' id (\() v -> SP.yield v) () big

speedFoldrS :: G.GiST Identity Vector (G.Within Int) Int Int -> Int
speedFoldrS big = SP.fst' $ runIdentity $ SP.sum stream
 where
  stream :: SP.Stream (SP.Of Int) Identity ()
  stream = G.foldrM id (\v r -> SP.yield v >> return r) () big

speedSearchS :: G.GiST Identity Vector (G.Within Int) Int Int -> Int
speedSearchS big = SP.fst' $ runIdentity $ SP.sum stream
 where
  stream :: SP.Stream (SP.Of Int) Identity ()
  stream = G.transformed $ G.search
    (VU.foldM_ (\() (_k, v) -> G.Transforming $ SP.yield v) ())
    (G.Within 0 maxBound)
    big

toList :: (G.IsGiST vec set k v, Monad m, G.Reads m r vec set k v) => set -> G.GiST r vec set k v -> m [(k, v)]
toList = G.search (return . VG.toList)

