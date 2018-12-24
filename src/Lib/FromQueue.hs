module Lib.FromQueue where

import qualified Control.Concurrent.STM.TBQueue as Q
import           Control.Monad.IO.Class (MonadIO, liftIO)
import           Streaming.Prelude (Stream, Of(..), yield)
import           Control.Concurrent.STM (atomically)

stream :: MonadIO m => (s -> a -> Either r s) -> Q.TBQueue a -> s -> Stream (Of a) m r
stream f q s = do
    next <- liftIO (atomically (Q.readTBQueue q))
    yield next
    either return (stream f q) (f s next)
