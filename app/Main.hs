module Main (main) where

import qualified Lib.Structures.Schemes.Example as ST
import qualified Lib.Structures.GiST.Example as Ex
import qualified Lib.Structures.GiST as G
import Data.Time.Clock (getCurrentTime)
import Control.Monad.Identity (runIdentity)
import qualified  Lib.Storage.System as SS

same :: a -> a -> ()
same _ _ = ()

main = SS.demoIO
  -- do
  -- print =<< getCurrentTime
  -- putStrLn "Creating 1"
  -- let !b = runIdentity $ Ex.bigSet'(G.FillFactor 16 32) 1000000 
  -- print =<< getCurrentTime
  -- putStrLn "Forcing"

  -- print $ Ex.force b
  -- print =<< getCurrentTime
  -- putStrLn "fold"

  -- print $ Ex.speedFold b
  -- print =<< getCurrentTime
  -- putStrLn "vfold"


  -- print $ Ex.speedFoldV b
  -- print =<< getCurrentTime
  -- putStrLn "sfold"

  -- print $ Ex.speedFoldlS b
  -- print =<< getCurrentTime
  -- putStrLn "ssearch"

  -- print $ Ex.speedSearchS b
  -- print =<< getCurrentTime
  -- putStrLn "done"
