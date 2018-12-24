module Lib.Writer (Offset, Writeback(..), save, Done(..), Write(..)) where

import           Streaming.Prelude (Stream, Of(..))
import qualified Streaming.Prelude as Stream
import           Control.Monad.IO.Class (MonadIO, liftIO)
import qualified Data.ByteString as ByteString
import           Data.ByteString (ByteString)
import           Data.Word (Word64)
import           System.IO (Handle)
import           Data.Profunctor (Profunctor, lmap, rmap)
import           Foreign.Storable (Storable)
-- import qualified Data.Vector.Storable as V

newtype Offset = Offset Word64 
    deriving newtype (Eq, Ord, Show, Num, Storable)

-- newtype Index = Index Int
--     deriving newtype (Eq, Ord, Show, Num)

newtype Done m = Done (m ())
instance Monad m => Semigroup (Done m) where
    Done a <> Done b = Done (a >> b)
instance Monad m => Monoid (Done m) where
    mempty = Done (return ())

data Write m o s = Write (o -> m ()) !s deriving Functor
instance Profunctor (Write m) where
    rmap = fmap
    lmap f (Write w s) = Write (w . f) s

data Writeback m o s = Writeback !(Write m o s) | Checkpoint (Done m) deriving Functor

-- save' :: MonadIO m => Handle -> Stream (Of ByteString) m a -> m a
-- save' hdl = Stream.mapM_ (liftIO . ByteString.hPut hdl)


save :: MonadIO m => Handle -> Stream (Of (Writeback m Offset ByteString)) m a -> m (Of Offset a)
save hdl = Stream.foldM step (return 0) return
    where
    step offset (Checkpoint (Done m)) = m >> return offset
    step offset (Writeback (Write done string)) = do
        liftIO $ ByteString.hPut hdl string
        done offset
        return (offset + Offset (fromIntegral $ ByteString.length string))

