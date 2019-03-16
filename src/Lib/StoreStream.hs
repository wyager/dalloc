module Lib.StoreStream where

-- import qualified Lib.Writer as Writer
-- import           Streaming.Prelude (Stream, Of(..))
-- import qualified Streaming.Prelude as Stream
import           Data.Store (Store, Size(VarSize, ConstSize), size, peek, poke, encode)
import           Data.Word (Word64)
import           Data.ByteString (ByteString)

data Sized a = Sized {sizedSize :: !Word64, getSized :: !a} 


instance Store a => Store (Sized a) where
    size = case size :: Size a of
        VarSize f -> VarSize (\(Sized _ a) -> 8 + f a)
        ConstSize i -> ConstSize (8 + i)
    poke (Sized s a) = poke s >> poke a
    peek = Sized <$> peek <*> peek


sized :: forall a . Store a => a -> Sized a
sized a = Sized (fromIntegral $ case size :: Size a of ConstSize i -> i; VarSize f -> f a) a

store :: Store a => a -> ByteString
store = encode . sized