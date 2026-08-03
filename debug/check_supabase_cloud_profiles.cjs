const { createClient } = require('@supabase/supabase-js');

const supabaseUrl = 'https://rwblykcpnduniexbivra.supabase.co';
const supabaseKey = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InJ3Ymx5a2NwbmR1bmlleGJpdnJhIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc4MTUxNzI0NCwiZXhwIjoyMDk3MDkzMjQ0fQ.Q04ibUleUEFCPcOsQ73qJI4W8nwDupfwACDeIczFAnw';

const supabase = createClient(supabaseUrl, supabaseKey);

async function run() {
  try {
    const { data: profiles, error } = await supabase
      .from('profiles')
      .select('id, email, full_name, profit_user');
      
    if (error) {
      console.error('Error fetching Supabase cloud profiles:', error);
      return;
    }
    
    console.log('--- Supabase Cloud Profiles Records ---');
    console.log(profiles);
  } catch (err) {
    console.error('Error:', err);
  }
}

run();
