module Main (main) where

import qualified Lib.SchemeTest as ST
import qualified Lib.GiST_ex as Ex
import qualified Lib.GiST as G
import           Data.Time.Clock (getCurrentTime)

main = do
    let b = Ex.bigSet (G.FillFactor 4 8) 200000 
    print =<< getCurrentTime
    putStrLn "Forcing"

    print $ Ex.force b
    print =<< getCurrentTime
    putStrLn "fold"

    print $ Ex.speedFold b
    print =<< getCurrentTime
    putStrLn "vfold"


    print $ Ex.speedFoldV b   
    print =<< getCurrentTime
    putStrLn "sfold"

    print $ Ex.speedFoldS b
    print =<< getCurrentTime
    putStrLn "ssearch"

    print $ Ex.speedSearchS b
    print =<< getCurrentTime
    putStrLn "done"
