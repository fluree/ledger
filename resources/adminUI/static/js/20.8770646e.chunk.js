(this["webpackJsonpfluree-admin-ui"]=this["webpackJsonpfluree-admin-ui"]||[]).push([[20],{1040:function(e,t,n){"use strict";n.r(t),n.d(t,"default",(function(){return b}));var r=n(151),c=n(4),s=n(0),i=n(1041),l=n(1033);function a(e){var t=e.value,n=e.name,r=e.tooltipText,s=void 0===r?"":r;return Object(c.jsxs)("div",{id:"network-screen-card",style:{borderRadius:"10px",display:"flex",flexDirection:"column",justifyContent:"center",backgroundColor:"#fff",height:"100%"},children:[s?Object(c.jsx)(i.a,{placement:"top",overlay:Object(c.jsx)(l.a,{style:{maxWidth:"auto",textAlign:"left"},children:s}),children:Object(c.jsx)("p",{style:{cursor:"pointer",color:"#091133",textAlign:"center"},children:n})}):Object(c.jsx)("p",{style:{color:"#091133",textAlign:"center"},children:n}),Object(c.jsx)("p",{style:{textAlign:"center",color:"#13c6ff",fontWeight:"bold",paddingTop:"10px"},children:t})]})}var o=n(462),d=n(463),j=n(464);function b(e){var t,n,i=e._db,l=i.networkData,b=l.index,x=l.leader,p=l.raft,u=(l.id,l.commit,l.term),h=l["svr-state"],m=(l["timeout-ms"],p.networks),g=(p.version,p["cmd-queue"]),O=m.map((function(e){return Object.keys(e)[0]})),f=O.length,v=p["new-db-queue"],k=i.db.split("/")[0],w=Object(s.useState)(k),N=Object(r.a)(w,2),y=N[0],T=N[1],A=m.filter((function(e){return e[y]}))[0][y].dbs,S=Object.keys(A).map((function(e){return{db:"".concat(y,"/").concat(e),status:A[e].status,block:A[e].block}})),L=g.filter((function(e){return y===Object.keys(e)[0]})).map((function(e){return e[y]}))[0],C=v.filter((function(e){return y===Object.keys(e)[0]})).map((function(e){return e[y]}))[0],q=function(e){return e.map((function(e,t){return Object(c.jsx)("th",{style:{textAlign:"center"},children:e},t)}))};return Object(c.jsxs)("div",{className:"network-page-wrapper",style:{textAlign:"center"},children:[Object(c.jsx)(o.a,{style:{marginTop:"20px",textAlign:"left",paddingLeft:"2%"},children:Object(c.jsxs)(d.a,{children:[Object(c.jsx)(j.a,{style:{marginRight:"4px"},children:"Network:"}),Object(c.jsxs)("select",{value:y,onChange:function(e){return T(e.target.value)},style:{marginLeft:"10px",borderRadius:"20px",border:"none",padding:"2px 4px",color:"#13C6FF",fontWeight:"bold"},placeholder:"Select Network",children:[Object(c.jsx)("option",{children:"Select Network"}),O.map((function(e){return Object(c.jsx)("option",{value:e,children:e})}))]})]})}),Object(c.jsxs)("div",{className:"network-page-main-content-wrapper",children:[Object(c.jsxs)("div",{className:"network-page-general-info",children:[Object(c.jsxs)("div",{className:"network-page-general-info-row",children:[Object(c.jsx)("div",{className:"network-page-general-info-row-item",children:Object(c.jsx)(a,{value:f,name:"Networks",tooltipText:"Number of networks present"})}),Object(c.jsx)("div",{className:"network-page-general-info-row-item",children:Object(c.jsx)(a,{value:x,name:"Leader",tooltipText:"Server providing the status"})})]}),Object(c.jsxs)("div",{className:"network-page-general-info-row",children:[Object(c.jsx)("div",{className:"network-page-general-info-row-item",children:Object(c.jsx)(a,{value:b,name:"Index",tooltipText:"Latest index of Server providing status"})}),Object(c.jsx)("div",{className:"network-page-general-info-row-item",children:Object(c.jsx)(a,{value:u,name:"Term",tooltipText:"Latest term in cycle"})})]}),Object(c.jsxs)("div",{className:"network-page-general-info-row",children:[Object(c.jsx)("div",{className:"network-page-general-info-row-item",children:Object(c.jsx)(a,{value:L||0,name:"Pending Transactions",tooltipText:"Number of Transactions in queue"})}),Object(c.jsx)("div",{className:"network-page-general-info-row-item",children:Object(c.jsx)(a,{value:C,name:"Pending Ledgers",tooltipText:"Number of pending new ledgers"})})]})]}),Object(c.jsxs)("div",{className:"network-page-table-wrapper",children:[Object(c.jsx)("div",{style:{paddingTop:"20px"},children:Object(c.jsxs)("div",{children:[Object(c.jsx)("h3",{style:{padding:"5px",textAlign:"left"},children:"Server Status"}),Object(c.jsx)("div",{id:"scroll-div-container",className:"network-page-table-container",children:Object(c.jsxs)("table",{className:"block-table block-table-stripes",striped:!0,bordered:!0,hover:!0,size:"sm",children:[Object(c.jsx)("thead",{style:{padding:"15px"},children:q(["Server","Status"])}),Object(c.jsx)("tbody",{children:(n=h,n.map((function(e){return Object(c.jsxs)("tr",{children:[Object(c.jsx)("td",{className:"table-content-centered Block",children:e.id}),Object(c.jsx)("td",{className:"table-content-centered ",children:e["active?"]?"Active":"Inactive"})]})})))})]})})]})}),Object(c.jsx)("div",{style:{paddingTop:"20px"},children:Object(c.jsxs)("div",{children:[Object(c.jsxs)("h3",{style:{padding:"5px",textAlign:"left"},children:["Ledgers In"," ",Object(c.jsx)("span",{style:{color:"#13c6ff"},children:Object(c.jsxs)("em",{children:['"',y,'"']})})," ","Network"]}),Object(c.jsx)("div",{className:"network-page-table-container",children:Object(c.jsxs)("table",{className:"block-table block-table-stripes",striped:!0,bordered:!0,hover:!0,size:"sm",children:[Object(c.jsx)("thead",{style:{padding:"15px"},children:q(["Ledgers","Status","Block"])}),Object(c.jsx)("tbody",{children:(t=S,t.map((function(e){return Object(c.jsxs)("tr",{children:[Object(c.jsx)("td",{className:"table-content-centered Block",children:e.db}),Object(c.jsx)("td",{className:"table-content-centered",children:e.status.toUpperCase()}),Object(c.jsx)("td",{className:"table-content-centered",children:e.block})]})})))})]})})]})})]})]})]})}}}]);
//# sourceMappingURL=20.8770646e.chunk.js.map