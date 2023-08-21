# LanceDB Chatbot - Vercel Next.js Template
Use an AI chatbot with website context retrieved from a vector store like LanceDB. LanceDB is lightweight and can be embedded directly into Next.js, with data stored on-prem.

## One click deploy on Vercel
[![Deploy with Vercel](https://vercel.com/button)](https://vercel.com/new/clone?repository-url=https%3A%2F%2Fgithub.com%2Flancedb%2Flancedb-vercel-chatbot&env=OPENAI_API_KEY&envDescription=OpenAI%20API%20Key%20for%20chat%20completion.&project-name=lancedb-vercel-chatbot&repository-name=lancedb-vercel-chatbot&demo-title=LanceDB%20Chatbot%20Demo&demo-description=Demo%20website%20chatbot%20with%20LanceDB.&demo-url=https%3A%2F%2Flancedb.vercel.app&demo-image=https%3A%2F%2Fi.imgur.com%2FazVJtvr.png)

![Demo website landing page](https://i.imgur.com/azVJtvr.png)

## Development

First, rename `.env.example` to `.env.local`, and fill out `OPENAI_API_KEY` with your OpenAI API key. You can get one [here](https://openai.com/blog/openai-api).

Run the development server:

```bash
npm run dev
# or
yarn dev
# or
pnpm dev
```

Open [http://localhost:3000](http://localhost:3000) with your browser to see the result.

This project uses [`next/font`](https://nextjs.org/docs/basic-features/font-optimization) to automatically optimize and load Inter, a custom Google Font.

## Learn More

To learn more about LanceDB or Next.js, take a look at the following resources:

- [LanceDB Documentation](https://lancedb.github.io/lancedb/) - learn about LanceDB, the developer-friendly serverless vector database.
- [Next.js Documentation](https://nextjs.org/docs) - learn about Next.js features and API.
- [Learn Next.js](https://nextjs.org/learn) - an interactive Next.js tutorial.

## LanceDB on Next.js and Vercel

FYI: these configurations have been pre-implemented in this template.

Since LanceDB contains a prebuilt Node binary, you must configure `next.config.js` to exclude it from webpack. This is required for both using Next.js and deploying on Vercel.
```js
/** @type {import('next').NextConfig} */
module.exports = ({
  webpack(config) {
    config.externals.push({ vectordb: 'vectordb' })
    return config;
  }
})
```

To deploy on Vercel, we need to make sure that the NodeJS runtime static file analysis for Vercel can find the binary, since LanceDB uses dynamic imports by default. We can do this by modifying `package.json` in the `scripts` section.
```json
{
  ...
  "scripts": {
    ...
    "vercel-build": "sed -i 's/nativeLib = require(`@lancedb\\/vectordb-\\${currentTarget()}`);/nativeLib = require(`@lancedb\\/vectordb-linux-x64-gnu`);/' node_modules/vectordb/native.js && next build",
    ...
  },
  ...
}
```
