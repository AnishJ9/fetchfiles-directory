"use client";

import { useEffect, useRef } from "react";

const REPO = "AnishJ9/fetchfiles-directory";
const REPO_ID = "R_kgDOSGVdug";
const CATEGORY = "General";
const CATEGORY_ID = "DIC_kwDOSGVdus4C7Kw-";

export function Comments({ term }: { term?: string }) {
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!ref.current || ref.current.childElementCount > 0) return;

    const script = document.createElement("script");
    script.src = "https://giscus.app/client.js";
    script.async = true;
    script.crossOrigin = "anonymous";
    script.setAttribute("data-repo", REPO);
    script.setAttribute("data-repo-id", REPO_ID);
    script.setAttribute("data-category", CATEGORY);
    script.setAttribute("data-category-id", CATEGORY_ID);
    script.setAttribute("data-mapping", term ? "specific" : "pathname");
    if (term) script.setAttribute("data-term", term);
    script.setAttribute("data-strict", "1");
    script.setAttribute("data-reactions-enabled", "1");
    script.setAttribute("data-emit-metadata", "0");
    script.setAttribute("data-input-position", "top");
    script.setAttribute("data-theme", "light");
    script.setAttribute("data-lang", "en");
    script.setAttribute("data-loading", "lazy");

    ref.current.appendChild(script);
  }, [term]);

  return (
    <div className="rounded-xl border border-ink-100 bg-white p-5">
      <h2 className="font-semibold text-ink-900 mb-3">Comments &amp; discussion</h2>
      <p className="text-sm text-ink-500 mb-4">
        Share tips, ask questions, or let others know how it was. Sign in with
        GitHub to post.
      </p>
      <div ref={ref} />
    </div>
  );
}
