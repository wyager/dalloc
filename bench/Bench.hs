import qualified Criterion.Main as CM
import qualified Criterion.Main.Options as CMO
import qualified Lib.Structures.GiST as G
import qualified Lib.Structures.GiST.Example as Gex
import qualified Lib.Storage.System as System
import qualified Data.Vector.Unboxed as VU



main :: IO ()
main = CM.runMode (CMO.Run CM.defaultConfig CMO.Prefix [""]) [ff 16 32]
 where
  ff min max = CM.bgroup (show (min, max)) (withFF (G.FillFactor min max))
  addUp ff = CM.env
    (return (Gex.bigSet ff 1000000))
    (\set -> CM.bgroup
      "add up 1,000,000 values in an Identity-backed GiST"
      [ CM.bench "plain foldl'" $ CM.whnf Gex.speedFold set
      , CM.bench "vec foldl'" $ CM.whnf Gex.speedFoldV set
      , CM.bench "stream foldl'" $ CM.whnf Gex.speedFoldlS set
      , CM.bench "stream foldr" $ CM.whnf Gex.speedFoldrS set
      , CM.bench "stream search" $ CM.whnf Gex.speedSearchS set
      ]
    )
  withFF ff@(G.FillFactor _min max_) =
    [ addUp ff
    -- , CM.bench "insert max elements into Identity-backed GiST" $ CM.nf (Gex.bigSet ff) max_
    -- , CM.bench "insert (max+1) elements into Identity-backed GiST" $ CM.nf (Gex.bigSet ff) (max_ + 1)
    -- , CM.bench "insert 100 elements into Identity-backed GiST" $ CM.nf (Gex.bigSet ff) 100
    , CM.bench "insert 1,000 elements into Identity-backed GiST" $ CM.nf (Gex.bigSet ff) 1000
    -- , CM.bench "insert 10,000 elements into Identity-backed GiST" $ CM.nf (Gex.bigSet ff) 10000
    -- , CM.bench "insert 100,000 elements into Identity-backed GiST" $ CM.nf (Gex.bigSet ff) 100000
    -- , CM.bench "insert 1,000,000 elements into Identity-backed GiST" $ CM.nf (Gex.bigSet ff) 1000000
    , CM.bench "insert 1,000 elements into In-memory Database backed GiST" $ CM.nfIO (dbTestMem ff 1000)
    , CM.bench "insert 1,000 elements into In-memory Database backed GiST" $ CM.nfIO (dbTestDisk ff 1000)
    ]
  dbTestMem :: G.FillFactor -> Int -> IO Int
  dbTestMem ff n = do
    (_filesystem, result) <- System.test mempty System.mockDBConfig $ do
      gist :: G.GiST System.Reft VU.Vector (G.Within Int) Int Int <- Gex.bigSet' ff n
      G.foldl' G.read (+) 0 gist
    return result
  dbTestDisk :: G.FillFactor -> Int -> IO Int
  dbTestDisk ff n = do
    let cfg = System.defaultDBConfig "/tmp/rundir"
    state <- System.setupIO @System.NoGC cfg 
    flip System.runDBT state $ do
      gist :: G.GiST System.Reft VU.Vector (G.Within Int) Int Int <- Gex.bigSet' ff n
      G.foldl' G.read (+) 0 gist
