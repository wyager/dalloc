import qualified Criterion.Main as CM
import qualified Criterion.Main.Options as CMO
import qualified Lib.GiST as G
import qualified Lib.GiST_ex as Gex



main :: IO ()
main = 
    CM.runMode (CMO.Run CM.defaultConfig CMO.Prefix [""]) 
        [ ff 16 32
        , ff 32 64
        ]
    where
    ff min max = CM.bgroup (show (min,max)) (withFF (G.FillFactor min max))
    withFF ff =
       [CM.bench "insert 100 elements into GiST" $ CM.nf (Gex.bigSet ff)  100
        , CM.bench "insert 1,000 elements into GiST" $ CM.nf (Gex.bigSet ff) 1000
        , CM.bench "insert 10,000 elements into GiST" $ CM.nf (Gex.bigSet ff) 10000
        , CM.bench "insert 100,000 elements into GiST" $ CM.nf (Gex.bigSet ff) 100000
        , CM.bench "insert 1,000,000 elements into GiST" $ CM.nf (Gex.bigSet ff) 1000000
        , CM.env (return (Gex.bigSet ff 1000000)) (\set -> CM.bgroup "add up 1,000,000 values in a GiST"
            [ CM.bench "plain foldl'"  $ CM.whnf Gex.speedFold  set
            , CM.bench "vec foldl'"    $ CM.whnf Gex.speedFoldV set
            , CM.bench "stream foldl'" $ CM.whnf Gex.speedFoldS set
            , CM.bench "stream search" $ CM.whnf Gex.speedSearchS set
            ])
        ]
