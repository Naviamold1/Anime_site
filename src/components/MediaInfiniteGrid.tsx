import { useEffect, useMemo, useRef, useState } from "react";
import { QueryClient, QueryClientProvider, useInfiniteQuery } from "@tanstack/react-query";
import { useWindowVirtualizer } from "@tanstack/react-virtual";

type MediaItem = {
  id: number;
  name?: string | null;
  englishName?: string | null;
  japaneseName?: string | null;
  type?: string | null;
  status?: string | null;
  episodes?: number | null;
  genres?: string | null;
  posterUrl?: string | null;
};

type ListPage = {
  items: MediaItem[];
  page: number;
  pageSize: number;
  total: number;
  totalPages: number;
};

type Props = {
  initialPage: ListPage;
  endpoint: string;
};

const MAX_LOADED_ITEMS = 1000;

function getColumnsFromWidth(width: number) {
  if (width >= 1280) return 4;
  if (width >= 1024) return 3;
  if (width >= 640) return 2;
  return 1;
}

function MediaInfiniteGridInner({ initialPage, endpoint }: Props) {
  const sentinelRef = useRef<HTMLDivElement | null>(null);
  const listRef = useRef<HTMLDivElement | null>(null);

  const [searchInput, setSearchInput] = useState("");
  const [debouncedSearch, setDebouncedSearch] = useState("");
  const [columns, setColumns] = useState(1);
  const [scrollMargin, setScrollMargin] = useState(0);

  useEffect(() => {
    const timer = setTimeout(() => {
      setDebouncedSearch(searchInput.trim());
    }, 300);

    return () => clearTimeout(timer);
  }, [searchInput]);

  useEffect(() => {
    const onResize = () => {
      setColumns(getColumnsFromWidth(window.innerWidth));
      if (listRef.current) {
        const top = listRef.current.getBoundingClientRect().top + window.scrollY;
        setScrollMargin(top);
      }
    };

    onResize();
    window.addEventListener("resize", onResize);
    return () => window.removeEventListener("resize", onResize);
  }, []);

  const query = useInfiniteQuery({
    queryKey: ["media-list", debouncedSearch],
    queryFn: async ({ pageParam }) => {
      const params = new URLSearchParams({ page: String(pageParam) });
      if (debouncedSearch) {
        params.set("q", debouncedSearch);
      }
      const response = await fetch(`${endpoint}?${params.toString()}`);
      if (!response.ok) {
        throw new Error("Failed to fetch media page");
      }
      return (await response.json()) as ListPage;
    },
    initialPageParam: 1,
    initialData: debouncedSearch
      ? undefined
      : {
          pages: [initialPage],
          pageParams: [1]
        },
    getNextPageParam: (lastPage) => {
      if (lastPage.page >= lastPage.totalPages) return undefined;
      return lastPage.page + 1;
    },
    staleTime: 60_000
  });

  const isSearching = debouncedSearch.length > 0;

  const rawItems = useMemo(() => {
    const all = query.data?.pages.flatMap((page) => page.items) ?? [];
    return isSearching ? all : all.slice(0, MAX_LOADED_ITEMS);
  }, [isSearching, query.data?.pages]);

  const hitMaxLoaded = !isSearching && rawItems.length >= MAX_LOADED_ITEMS;

  useEffect(() => {
    const sentinel = sentinelRef.current;
    if (!sentinel) return;

    const observer = new IntersectionObserver(
      (entries) => {
        const entry = entries[0];
        if (!entry?.isIntersecting) return;
        if (hitMaxLoaded) return;
        if (!query.hasNextPage || query.isFetchingNextPage) return;
        void query.fetchNextPage();
      },
      { rootMargin: "500px 0px" }
    );

    observer.observe(sentinel);
    return () => observer.disconnect();
  }, [hitMaxLoaded, query.fetchNextPage, query.hasNextPage, query.isFetchingNextPage]);

  const currentTotal = query.data?.pages[0]?.total ?? initialPage.total;
  const rowCount = Math.ceil(rawItems.length / columns);

  const virtualizer = useWindowVirtualizer({
    count: rowCount,
    estimateSize: () => 460,
    overscan: 3,
    scrollMargin
  });

  const virtualRows = virtualizer.getVirtualItems();
  const accentStatus = query.isFetchingNextPage
    ? "Loading more episodes..."
    : hitMaxLoaded
      ? `Reached performance cap (${MAX_LOADED_ITEMS.toLocaleString()} loaded)`
      : query.hasNextPage
        ? "Keep scrolling for more"
        : "End of catalog";

  return (
    <section className="relative overflow-hidden bg-[#090909] text-zinc-100">
      <div className="pointer-events-none absolute inset-0 bg-[radial-gradient(circle_at_top,_rgba(255,98,0,0.22),_transparent_48%),radial-gradient(circle_at_80%_30%,_rgba(255,255,255,0.08),_transparent_36%)]" />
      <div className="pointer-events-none absolute inset-x-0 top-0 h-32 bg-gradient-to-b from-black/70 to-transparent" />

      <div className="relative mx-auto flex w-full max-w-7xl flex-col gap-6 px-4 py-8 sm:px-6">
        <header className="rounded-2xl border border-zinc-800/80 bg-gradient-to-r from-black/70 via-zinc-900/80 to-black/60 p-6 shadow-2xl shadow-black/40 backdrop-blur">
          <p className="text-xs uppercase tracking-[0.22em] text-orange-400">Streaming Catalog</p>
          <h1 className="mt-2 text-3xl font-semibold tracking-tight sm:text-4xl">Anime &amp; Manga Library</h1>
          <p className="mt-3 text-sm text-zinc-300">
            Showing {rawItems.length.toLocaleString()} of {currentTotal.toLocaleString()} results from
            <code className="ml-1 rounded bg-zinc-900 px-1.5 py-0.5 text-zinc-100">src/db/media.db</code>.
          </p>
          <p className="mt-1 text-sm text-orange-300">{accentStatus}</p>
        </header>

        <div className="rounded-xl border border-zinc-800 bg-black/40 p-4 backdrop-blur">
          <label htmlFor="search" className="mb-2 block text-xs font-medium uppercase tracking-[0.14em] text-zinc-400">
            Find a title
          </label>
          <input
            id="search"
            type="search"
            value={searchInput}
            onChange={(event) => setSearchInput(event.target.value)}
            placeholder="Search by title, genre, type..."
            autoComplete="off"
            className="w-full rounded-lg border border-zinc-700 bg-zinc-950/90 px-3 py-2 text-sm text-zinc-100 outline-none placeholder:text-zinc-500 focus-visible:border-orange-500 focus-visible:ring-2 focus-visible:ring-orange-500/40"
          />
        </div>

        {query.isError ? (
          <p className="rounded-xl border border-red-400/40 bg-red-500/10 p-4 text-sm text-red-200">
            Failed to load media list.
          </p>
        ) : null}

        <div ref={listRef} className="relative" style={{ height: `${virtualizer.getTotalSize()}px` }}>
          {virtualRows.map((virtualRow) => {
            const start = virtualRow.index * columns;
            const rowItems = rawItems.slice(start, start + columns);

            return (
              <div
                key={virtualRow.key}
                className="absolute left-0 top-0 w-full"
                style={{ transform: `translateY(${virtualRow.start - scrollMargin}px)` }}
              >
                <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4">
                  {rowItems.map((item) => {
                    const title = item.name || item.englishName || item.japaneseName || `#${item.id}`;
                    const secondaryName =
                      item.englishName && item.englishName !== item.name ? item.englishName : item.japaneseName;
                    const details = [item.type, item.status, item.episodes ? `${item.episodes} eps` : ""]
                      .filter(Boolean)
                      .join(" • ");

                    return (
                      <article
                        key={item.id}
                        className="group overflow-hidden rounded-xl border border-zinc-800 bg-zinc-950/80 shadow-lg shadow-black/35 transition duration-200 hover:-translate-y-1 hover:border-orange-500/50"
                      >
                        <div className="relative">
                          <img
                            src={item.posterUrl || "https://placehold.co/600x900?text=No+Poster"}
                            alt={title}
                            loading="lazy"
                            className="aspect-[2/3] w-full object-cover transition duration-300 group-hover:scale-[1.03]"
                          />
                          <div className="pointer-events-none absolute inset-0 bg-gradient-to-t from-black/90 via-black/15 to-transparent" />
                          {item.type ? (
                            <span className="absolute left-2 top-2 rounded-full bg-black/70 px-2 py-1 text-[10px] font-semibold uppercase tracking-wider text-orange-300 ring-1 ring-orange-400/40">
                              {item.type}
                            </span>
                          ) : null}
                        </div>
                        <div className="space-y-2 p-3">
                          <h2 className="line-clamp-2 text-sm font-semibold leading-snug text-zinc-100">{title}</h2>
                          {secondaryName ? (
                            <p className="line-clamp-1 text-xs text-zinc-400">{secondaryName}</p>
                          ) : null}
                          {details ? <p className="text-xs text-zinc-300">{details}</p> : null}
                          {item.genres ? <p className="line-clamp-2 text-xs text-zinc-400">{item.genres}</p> : null}
                        </div>
                      </article>
                    );
                  })}
                </div>
              </div>
            );
          })}
        </div>

        <div className="flex justify-center">
          <div ref={sentinelRef} className="h-8 w-full" aria-hidden="true" />
          <p className="rounded-full border border-zinc-700 bg-zinc-900/80 px-4 py-1.5 text-xs uppercase tracking-wider text-zinc-300">
            {accentStatus}
          </p>
        </div>
      </div>
    </section>
  );
}

export default function MediaInfiniteGrid(props: Props) {
  const [queryClient] = useState(
    () =>
      new QueryClient({
        defaultOptions: {
          queries: {
            refetchOnWindowFocus: false
          }
        }
      })
  );

  return (
    <QueryClientProvider client={queryClient}>
      <MediaInfiniteGridInner {...props} />
    </QueryClientProvider>
  );
}
