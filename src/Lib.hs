module Lib where

import Data.Word (Word64)
import Data.ByteString (ByteString)
import Data.Store (Store, peek, poke)
import Data.Fix (Fix)
import qualified Data.Store as Store


newtype Gen = Gen Word64 deriving newtype Store
newtype Ix = Ix Word64 deriving newtype Store

data Ptr = Ptr !Gen !Ix

instance Store Ptr where
    size = Store.ConstSize 16
    poke (Ptr gen ix) = poke gen >> poke ix
    peek = Ptr <$> peek <*> peek

class StoreM m where
    store :: ByteString -> m Ptr

-- f :: (Monad m, StoreM m, (forall a . Store a => Store (f a))) => m (Fix f)
-- f = undefined

-- data Chunk = ChunkPtr Ptr | ChunkBS ByteString

-- class Write m where
--     write :: Stream (Of Chunk) m () -> m Ptr

-- class Stream a where
--     stream :: a -> Stream (Of (Either ByteString Ptr)) 

-- class Emitter m where
--     raw :: ByteString -> m ()
--     ptr :: Ptr -> m ()

-- newtype Takeout f g = Takeout {takeout :: f g}

-- taken :: Traversable f => f g -> ([g], f ())
-- taken = mapAccumR (\list g -> (g:list, ())) [] 

-- packed :: Traversable f => [g] -> f a -> Maybe (f g)
-- packed gs f = 
--     case State.runStateT (mapM (const place) f) gs of
--         Nothing -> Nothing
--         Just (packed, remaining) -> if null remaining then Just packed else Nothing
--     where
--     place = State.get >>= \case
--         [] -> State.lift Nothing
--         (item : items) -> State.put items >> return item

-- instance (Traversable f, (forall a . Serialize a => Serialize (f a)), Serialize g) => Serialize (Takeout f g) where
--     put (Takeout f) = Serialize.put (taken f)
--     get = do
--         (gs,fa :: f ()) <- Serialize.get
--         case packed gs fa of
--             Nothing -> Fail.fail "Mismatched number of packed arguments"
--             Just fg -> return (Takeout fg)

-- emit :: (Traversable f, forall a . Serialize a => Serialize (f a), Emitter m) => f Ptr -> m ()
-- emit f = 