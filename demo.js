import { TableProxy } from './db-proxy.js'
const Info = await new TableProxy({dbUrl: process.env.DATABASE_URL, table: 'info', logging: true})
console.log(Info.rows)
Info.rows[0].extra.filesInfo.end = 'today'
setTimeout(()=>console.log(Info.rows), 1000)