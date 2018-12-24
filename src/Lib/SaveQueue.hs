module Lib.SaveQueue where

import qualified Control.Concurrent.STM.TBQueue as Q
import           Control.Monad.IO.Class (MonadIO, liftIO)
import           Lib.FromQueue (stream)
import           Lib.Agglomerator (agglomerate, Particle(Slippery,Sticky,Unstick))
import           Lib.Writer (save, Writeback(Writeback, Checkpoint), Done(..), Write(..), Offset)
import qualified Streaming.Prelude as Stream
import           Streaming.Prelude (Of((:>)))
import           System.IO (Handle, hFlush)
import           Data.ByteString (ByteString)
import qualified Data.ByteString as ByteString


data Enqueued m = QFlush | QWrite !(Writeback m Offset ByteString)

data QueueSize = QueueSize {
        queueBytes :: Int,
        queueMessages :: Int
    }

bump :: QueueSize -> ByteString -> QueueSize
bump QueueSize{..} bs = QueueSize (queueBytes + ByteString.length bs) (queueMessages + 1)

exceeds :: QueueSize -> QueueSize -> Bool
s1 `exceeds` s2 = (queueBytes s1 > queueBytes s2) || (queueMessages s1 > queueMessages s2)

limitTo :: QueueSize -> QueueSize -> Enqueued m -> Either () QueueSize
limitTo maxSize = \currentSize enqueued -> case enqueued of
    QFlush -> Right currentSize
    QWrite writeback -> case writeback of
        Checkpoint _ -> Right currentSize
        Writeback (Write _ bs) -> if newSize `exceeds` maxSize then Left () else Right newSize
            where newSize = bump currentSize bs

saveQ :: forall m . MonadIO m => QueueSize -> Q.TBQueue (Enqueued m) -> Handle -> m Offset
saveQ hdlLimit q hdl = do
    offset :> () <- save hdl $ Stream.map unparticulize 
                             $ agglomerate 
                             $ Stream.map particulize 
                             $ stream (limitTo hdlLimit) q (QueueSize 0 0) >> Stream.yield QFlush
    return offset
    where
    particulize :: Enqueued m -> Particle (Done m) (Write m Offset ByteString)
    particulize QFlush = Unstick
    particulize (QWrite (Writeback w)) = Slippery w
    particulize (QWrite (Checkpoint d)) = Sticky d
    unparticulize :: Either (Done m) (Write m Offset ByteString) -> Writeback m Offset ByteString
    unparticulize (Left (Done done)) = Checkpoint (Done (liftIO (hFlush hdl) >> done))
    unparticulize (Right w) = Writeback w