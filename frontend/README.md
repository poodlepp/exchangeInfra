# frontend

此目录预留给 CEX 项目的前端工程,后端脚手架不会在此创建任何代码。

## 建议技术栈

- 构建工具: Vite 5+
- 框架: React 18+ 或 Vue 3+
- 语言: TypeScript 5+
- 包管理: pnpm(推荐)/ npm / yarn
- 状态管理: Zustand / Pinia
- UI 组件库: Ant Design / Element Plus / shadcn-ui
- 网络请求: axios / ky
- 行情图表: TradingView Lightweight Charts / klinecharts

## 初始化示例

```bash
cd frontend

# React + Vite + TS
pnpm create vite@latest . --template react-ts

# 或 Vue + Vite + TS
pnpm create vite@latest . --template vue-ts

pnpm install
pnpm dev
```

## 与后端联调

后端默认监听 `http://localhost:8080`,Swagger 文档位于 `/swagger-ui.html`。开发环境可在 Vite 中配置代理:

```ts
// vite.config.ts
export default defineConfig({
  server: {
    proxy: {
      '/api': { target: 'http://localhost:8080', changeOrigin: true },
    },
  },
});
```
