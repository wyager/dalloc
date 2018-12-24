module Lib.Dalloc where

-- import           Data.Profunctor (Profunctor, lmap, rmap)

-- data Request m i o = Flush (m ()) | Write (i -> m ()) !o
-- instance Profunctor (Request m) where
--     lmap _ (Flush m)   = Flush m
--     lmap f (Write g o) = Write (g . f) o
--     rmap _ (Flush m)   = Flush m
--     rmap f (Write g o) = Write g (f o)

-- master :: Monad m => m Request -> m void
-- master next = do
--     req <- next
--     case req of
--         Flush



-- master = do
--     extant <- initFrom path
