require('dotenv').config({ path: '../profit-web/.env' });

fetch(`${process.env.PUBLIC_SUPABASE_URL}/rest/v1/branches?select=name,agent_url`, {
    headers: {
        'apikey': process.env.PUBLIC_SUPABASE_ANON_KEY,
        'Authorization': `Bearer ${process.env.PUBLIC_SUPABASE_ANON_KEY}`
    }
})
.then(res => res.json())
.then(data => {
    console.log(data);
    process.exit(0);
})
.catch(err => console.error(err));
