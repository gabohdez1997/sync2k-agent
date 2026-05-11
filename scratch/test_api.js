// Simula exactamente lo que hace AgentClient para GET /articulos/:co_art
async function test() {
    const co_art = '0101001012';
    const url = `http://localhost:3000/api/v1/articulos/${encodeURIComponent(co_art)}`;
    console.log("Fetching:", url);
    
    try {
        const response = await fetch(url, {
            headers: {
                'Content-Type': 'application/json',
                'x-api-key': 'galpe123'  // Ajustar según tu .env
            }
        });
        console.log("Status:", response.status, response.statusText);
        const data = await response.json();
        console.log("Full Response:", JSON.stringify(data, null, 2));
        
        // Simular la lógica de +page.server.ts
        if (data.success !== false) {
            const articleData = data.data || data;
            console.log("\narticleData type:", typeof articleData, Array.isArray(articleData) ? 'ARRAY' : 'NOT ARRAY');
            console.log("articleData length:", Array.isArray(articleData) ? articleData.length : 'N/A');
            
            let rawArticle = null;
            if (Array.isArray(articleData) && articleData.length > 0) {
                rawArticle = articleData[0];
            } else if (!Array.isArray(articleData)) {
                rawArticle = articleData;
            }
            
            if (rawArticle) {
                const article = {
                    ...rawArticle,
                    art_des: rawArticle.descripcion || rawArticle.art_des,
                    tipo: rawArticle.tipo_articulo || rawArticle.tipo,
                    uni_venta: rawArticle.co_uni || rawArticle.uni_venta,
                };
                console.log("\n=== FINAL MAPPED ARTICLE ===");
                console.log("co_art:", article.co_art);
                console.log("art_des:", article.art_des);
                console.log("tipo:", article.tipo);
                console.log("co_lin:", article.co_lin);
                console.log("co_subl:", article.co_subl);
                console.log("co_cat:", article.co_cat);
                console.log("co_color:", article.co_color);
                console.log("co_uni/uni_venta:", article.uni_venta);
                console.log("tipo_imp:", article.tipo_imp);
                console.log("precios:", article.precios);
            } else {
                console.log("rawArticle is NULL!");
            }
        } else {
            console.log("success === false:", data);
        }
    } catch(e) {
        console.error("FETCH ERROR:", e.message);
    }
}
test();
