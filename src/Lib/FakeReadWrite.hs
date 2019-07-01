module Lib.FakeReadWrite where

import Data.Typeable (Typeable)
import Data.Dynamic (Dynamic, toDyn, fromDynamic)
import Data.Map.Strict (Map, insert, lookupMax)
import Control.Monad.State.Strict (State)
import Control.Monad.State.Class (MonadState, get, modify)

data BS = BS Dynamic

-- FakeSerialize

class FakS a where
    code :: a -> BS
    decode :: BS -> Maybe a
    default code :: Typeable a => a -> BS
    code = BS . toDyn
    default decode :: Typeable a => BS -> Maybe a
    decode (BS a) = fromDynamic a

data FakR a = FakR Int

data FakW a = FakW Int

newtype FakM a = FakM (State (Map Int BS) a) 
    deriving newtype (Functor, Applicative, Monad, MonadState (Map Int BS))


save :: FakS a => a -> FakM (FakW a)
save a = do
    fs <- get
    let n = case lookupMax fs of 
            Nothing -> 0
            Just (k,_) -> k + 1
    modify (insert n (code a))
    return (FakW n)


