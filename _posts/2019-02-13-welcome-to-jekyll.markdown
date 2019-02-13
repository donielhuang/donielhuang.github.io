---
layout: post
title:  "hello jekyll!"
date:   2015-02-10 15:14:54
categories: jekyll
comments: true
---


#### 目錄架構

jekyll 的目錄結構如下，文章主要放在 _post 目錄裡，圖片、js、css 之類的放在 static 目錄裡．  

![jekyll_1.jpg](/static/img/jekyll_1.jpg){:height="400px" width="600px"}

寫文章時要先定義好標頭，layout 會對應到 _layouts 目錄，categories 用來對文章進行分類．

```
---
layout: post
title:  "hello jekyll"
date:   2015-02-10 15:14:54
categories: jekyll
comments: true
---

```

再使用 [git page](https://pages.github.com/)，把 project commit 到 repositories，畫面結果如下．

![jekyll_2.jpg](/static/img/jekyll_2.jpg){:height="400px" width="600px"}
