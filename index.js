import { TableProxy } from './db-proxy.js'
const Info = await new TableProxy({dbUrl: process.env.DATABASE_URL, table: 'info', logging: true})
