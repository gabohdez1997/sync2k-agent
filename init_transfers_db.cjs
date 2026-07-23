require('dotenv').config();
const pg = require('pg');

const pgUrl = process.env.LOCAL_PG_URL || 'postgresql://postgres:Galpe2021*@localhost:5432/sync2k';
const pgPool = new pg.Pool({ connectionString: pgUrl });

async function initTables() {
    console.log("🔗 Verificando/creando tablas de traslados en PG local:", pgUrl);
    await pgPool.query(`
        CREATE TABLE IF NOT EXISTS stock_transfers (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            transfer_number VARCHAR(30) UNIQUE NOT NULL,
            source_branch_id UUID REFERENCES branches(id),
            target_branch_id UUID REFERENCES branches(id),
            source_ajue_num VARCHAR(20),
            target_ajue_num VARCHAR(20),
            status VARCHAR(20) DEFAULT 'TRANSITO',
            motivo TEXT,
            created_by TEXT,
            accepted_by TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW(),
            accepted_at TIMESTAMPTZ
        );

        CREATE TABLE IF NOT EXISTS stock_transfer_items (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            transfer_id UUID REFERENCES stock_transfers(id) ON DELETE CASCADE,
            co_art VARCHAR(30) NOT NULL,
            art_des TEXT NOT NULL,
            co_alma_source VARCHAR(6) NOT NULL,
            co_alma_target VARCHAR(6) NOT NULL,
            total_art NUMERIC(18,5) NOT NULL DEFAULT 0,
            accepted_art NUMERIC(18,5) NOT NULL DEFAULT 0,
            costo_unit NUMERIC(18,5) NOT NULL DEFAULT 0,
            co_uni VARCHAR(10) DEFAULT 'UND'
        );
        ALTER TABLE stock_transfer_items ADD COLUMN IF NOT EXISTS co_uni VARCHAR(10) DEFAULT 'UND';
    `);
    console.log("✅ Tablas stock_transfers y stock_transfer_items listas en PostgreSQL local.");
    process.exit(0);
}

initTables().catch(e => { console.error("❌ Error creando tablas:", e); process.exit(1); });
