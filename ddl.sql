CREATE TABLE IF NOT EXISTS users (
  id serial PRIMARY KEY,
  email text NOT NULL UNIQUE,
  PASSWORD text NOT NULL,
  active boolean NOT NULL DEFAULT false,
  created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  activated_at timestamp
);

CREATE TABLE IF NOT EXISTS tokens (
  id serial PRIMARY KEY,
  token text NOT NULL,
  user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS messages (
  id serial PRIMARY KEY,
  message text NOT NULL,
  user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  private boolean NOT NULL DEFAULT false,
  created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at timestamp
);

CREATE TABLE IF NOT EXISTS upvotes (
  id serial PRIMARY KEY,
  user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  message_id INTEGER NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
  created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE OR REPLACE VIEW popular_messages AS
SELECT m.id,
  m.message,
  COUNT(upvotes.id) AS upvotes
FROM messages m
  LEFT JOIN upvotes ON m.id = upvotes.message_id
GROUP BY m.id,
  m.message
ORDER BY COUNT(upvotes.id) DESC
LIMIT 10;