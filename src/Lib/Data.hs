{-# LANGUAGE UndecidableInstances #-}
module Lib.Data where


import           Lib.GiSTf (Hoist)
import           Lib.System (ByteSemantics, semanticIsRoot, semanticChildren, IsRoot(NotRoot,YesRoot), NoParse(..), Ref)
import           Data.ByteString (ByteString)
import           Data.Map.Strict (Map)
import           Data.Store (Store, Size(ConstSize,VarSize), size, peek, poke, decode)
import           GHC.Generics (Generic)
import           Data.Functor.Const (Const)
import           Data.Vector.Storable (fromList, empty)
import           Data.Foldable (toList)
import           Data.Coerce (coerce)

data Tag = Node' | Leaf'
    deriving stock Generic
    deriving anyclass Store


sizeOf :: Store a => a -> Int
sizeOf x = case size of
                ConstSize i -> i
                VarSize f -> f x


data Ser (f :: * -> *) (a :: Tag) where
    Node :: Map ByteString (f (SomeSer f)) -> Ser f 'Node'
    Leaf :: ByteString -> Ser f 'Leaf'

data SomeSer f where
    SomeSer :: Ser f a -> SomeSer f

instance (Hoist Store f) => Store (SomeSer f) where
    size = VarSize $ \(SomeSer s) -> case s of
                        Node theMap -> sizeOf Node' + sizeOf theMap
                        Leaf bs -> sizeOf Leaf' + sizeOf bs
    peek = do
        tag <- peek
        case tag of
            Node' -> SomeSer . Node <$> peek
            Leaf' -> SomeSer . Leaf <$> peek
    poke (SomeSer s) = case s of
        Node theMap -> do
            poke Node'
            poke theMap
        Leaf bs -> do
            poke Leaf'
            poke bs

instance ByteSemantics (SomeSer (Const Ref)) where
    semanticIsRoot _ bs = case decode @(SomeSer (Const Ref)) bs of -- Optimize: Only look at tag
        Left ex -> Left (NoParse $ show ex)
        Right (SomeSer s) -> case s of
            Node _ -> Right YesRoot
            Leaf _ -> Right NotRoot
    semanticChildren _ bs = case decode @(SomeSer (Const Ref)) bs of
        Left ex -> Left (NoParse $ show ex)
        Right (SomeSer s) -> case s of
            Node theMap -> Right . coerce . fromList . toList $ theMap
            Leaf _ -> Right empty

-- go r@(Root theMap) = 
-- go l@(Leaf _) = save (encode l)