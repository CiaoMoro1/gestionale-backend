create extension if not exists "uuid-ossp";

create table products (
  id uuid primary key default uuid_generate_v4(),
  sku text not null unique,
  name text not null,
  quantity int not null default 0,
  shopify_variant_id text,
  shopify_product_id text,
  user_id uuid references auth.users(id)
);

create table movements (
  id bigserial primary key,
  product_id uuid references products(id),
  delta int not null,
  source text not null,
  "timestamp" timestamptz default now(),
  user_id uuid references auth.users(id)
);

alter table products  enable row level security;
alter table movements enable row level security;

create policy "user can read own"
  on products for select using (user_id = auth.uid());
create policy "user can modify own"
  on products for update using (user_id = auth.uid());
create policy "user can insert own"
  on products for insert with check (user_id = auth.uid());
create policy "user can delete own"
  on products for delete using (user_id = auth.uid());

create or replace function apply_delta(id uuid, d int)
returns void language plpgsql as $$
begin
  update products
  set quantity = quantity + d
  where products.id = id;

  insert into movements(product_id, delta, source, user_id)
  values (id, d, 'manual', auth.uid());
end;
$$;
