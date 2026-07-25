import {
  describe,
  expect,
  it
} from "vitest";

import { normalizeArticleUrl } from "../src/url-normalization.js";

describe("normalizeArticleUrl", () => {
  it("removes approved tracking parameters while preserving identity-significant query data", () => {
    const result = normalizeArticleUrl("HTTPS://Articles.Example.TEST:443/news/story/?utm_source=feed&id=2&ref=home&id=1#comments");

    expect(result).toEqual({
      ok: true,
      value: {
        url: "https://articles.example.test/news/story?id=1&id=2",
        removedParameters: [
          "utm_source",
          "ref"
        ]
      }
    });
  });

  it("sorts retained query parameters deterministically", () => {
    const result = normalizeArticleUrl("https://articles.example.test/news/story?z=last&page=2&a=first&utm_medium=email");

    expect(result).toEqual({
      ok: true,
      value: {
        url: "https://articles.example.test/news/story?a=first&page=2&z=last",
        removedParameters: [
          "utm_medium"
        ]
      }
    });
  });

  it("rejects URLs that are not safe article fetch identities", () => {
    expect(normalizeArticleUrl("not a url")).toEqual({
      ok: false,
      reason: "invalid-url"
    });
    expect(normalizeArticleUrl("ftp://articles.example.test/news/story")).toEqual({
      ok: false,
      reason: "unsupported-scheme"
    });
    expect(normalizeArticleUrl("https://user@articles.example.test/news/story")).toEqual({
      ok: false,
      reason: "credentialed-url"
    });
  });
});
