CREATE TABLE catalog.db.constraints_schema (
  id UUID PRIMARY KEY,
  status TEXT DEFAULT 'pending',
  user_id UUID CONSTRAINT fk_user_id REFERENCES users (id)
); 