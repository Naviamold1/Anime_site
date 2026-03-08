import { useState, useEffect } from "react";
import { useSession } from "@/lib/auth-client";

type Comment = {
	id: string;
	content: string;
	createdAt: string;
	user: {
		id: string;
		name: string;
		image: string | null;
	};
};

type RatingData = {
	stats: {
		average: string | null;
		total: number;
	};
	userRating: number | null;
};

export default function ReviewSection({ mediaId }: { mediaId: number }) {
	const { data: session } = useSession();
	const [activeTab, setActiveTab] = useState<"rating" | "comments">("rating");

	const [ratingData, setRatingData] = useState<RatingData | null>(null);
	const [comments, setComments] = useState<Comment[]>([]);
	const [newComment, setNewComment] = useState("");
	const [isSubmitting, setIsSubmitting] = useState(false);

	useEffect(() => {
		fetch(`/api/media/${mediaId}/rating.json`)
			.then((res) => res.json())
			.then((data) => setRatingData(data))
			.catch(console.error);

		fetch(`/api/media/${mediaId}/comments.json`)
			.then((res) => res.json())
			.then((data) => setComments(data))
			.catch(console.error);
	}, [mediaId, session]); // re-fetch if session changes (e.g. login)

	const handleRate = async (score: number) => {
		if (!session) return alert("Please sign in to rate!");

		// Optimistic update
		setRatingData((prev) =>
			prev
				? {
						...prev,
						userRating: score,
						stats: {
							...prev.stats,
							total: prev.userRating ? prev.stats.total : prev.stats.total + 1,
						},
					}
				: null,
		);

		await fetch(`/api/media/${mediaId}/rating.json`, {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify({ score }),
		});

		// Refetch to get actual new average
		const res = await fetch(`/api/media/${mediaId}/rating.json`);
		setRatingData(await res.json());
	};

	const handlePostComment = async (e: React.FormEvent) => {
		e.preventDefault();
		if (!session || !newComment.trim()) return;

		setIsSubmitting(true);
		try {
			const res = await fetch(`/api/media/${mediaId}/comments.json`, {
				method: "POST",
				headers: { "Content-Type": "application/json" },
				body: JSON.stringify({ content: newComment }),
			});

			if (res.ok) {
				const comment = await res.json();
				setComments([comment, ...comments]);
				setNewComment("");
			}
		} catch (err) {
			console.error(err);
		} finally {
			setIsSubmitting(false);
		}
	};

	return (
		<div className="mt-12 rounded-xl bg-zinc-900/50 p-6 ring-1 ring-zinc-800/50">
			<div className="mb-6 flex gap-6 border-b border-zinc-800">
				<button
					onClick={() => setActiveTab("rating")}
					className={`pb-3 text-sm font-semibold transition-colors ${activeTab === "rating" ? "border-b-2 border-orange-400 text-orange-400" : "text-zinc-400 hover:text-zinc-200"}`}
				>
					Ratings & Reviews
				</button>
				<button
					onClick={() => setActiveTab("comments")}
					className={`pb-3 text-sm font-semibold transition-colors ${activeTab === "comments" ? "border-b-2 border-orange-400 text-orange-400" : "text-zinc-400 hover:text-zinc-200"}`}
				>
					Community Comments ({comments.length})
				</button>
			</div>

			{activeTab === "rating" && (
				<div className="flex flex-col items-start gap-8 sm:flex-row">
					<div className="flex min-w-[150px] flex-col items-center justify-center rounded-xl bg-black/40 p-6 ring-1 ring-zinc-800">
						<span className="text-4xl font-extrabold text-white">
							{ratingData?.stats.average || "—"}
						</span>
						<span className="mt-1 text-sm text-zinc-500">out of 10</span>
						<span className="mt-2 text-xs text-zinc-600">
							{ratingData?.stats.total || 0} Ratings
						</span>
					</div>

					<div className="flex-1">
						<h4 className="mb-2 text-lg font-bold text-white">Rate this Anime</h4>
						{session ? (
							<div>
								<div className="mb-4 flex flex-wrap gap-2">
									{[1, 2, 3, 4, 5, 6, 7, 8, 9, 10].map((score) => (
										<button
											key={score}
											onClick={() => handleRate(score)}
											className={`h-10 w-10 rounded-full font-bold transition-all ${ratingData?.userRating === score ? "scale-110 bg-orange-500 text-white" : "bg-zinc-800 text-zinc-400 hover:bg-zinc-700"}`}
										>
											{score}
										</button>
									))}
								</div>
								{ratingData?.userRating && (
									<p className="text-sm text-green-400">
										You rated this {ratingData.userRating}/10.
									</p>
								)}
							</div>
						) : (
							<p className="text-sm text-zinc-400">Please sign in to leave a rating.</p>
						)}
					</div>
				</div>
			)}

			{activeTab === "comments" && (
				<div>
					{session ? (
						<form onSubmit={handlePostComment} className="mb-8">
							<textarea
								value={newComment}
								onChange={(e) => setNewComment(e.target.value)}
								placeholder="Share your thoughts..."
								className="min-h-[100px] w-full resize-y rounded-lg border border-zinc-800 bg-zinc-950 p-3 text-sm text-white focus:border-orange-500 focus:ring-1 focus:ring-orange-500 focus:outline-none"
								required
							/>
							<div className="mt-2 flex justify-end">
								<button
									type="submit"
									disabled={isSubmitting || !newComment.trim()}
									className="rounded-md bg-orange-500 px-6 py-2 text-sm font-bold text-white transition-colors hover:bg-orange-600 disabled:opacity-50"
								>
									{isSubmitting ? "Posting..." : "Post Comment"}
								</button>
							</div>
						</form>
					) : (
						<div className="mb-8 rounded-lg border border-zinc-800 bg-zinc-950 p-4 text-center">
							<p className="text-sm text-zinc-400">Sign in to join the conversation.</p>
						</div>
					)}

					<div className="space-y-4">
						{comments.length === 0 ? (
							<p className="py-4 text-center text-sm text-zinc-500">
								No comments yet. Be the first!
							</p>
						) : (
							comments.map((comment) => (
								<div key={comment.id} className="flex gap-4 rounded-lg bg-zinc-950/50 p-4">
									<div className="flex h-10 w-10 shrink-0 items-center justify-center overflow-hidden rounded-full bg-zinc-800 font-bold text-zinc-400">
										{comment.user.image ? (
											<img
												src={comment.user.image}
												alt={comment.user.name}
												className="h-full w-full object-cover"
											/>
										) : (
											comment.user.name.charAt(0).toUpperCase()
										)}
									</div>
									<div className="flex-1">
										<div className="mb-1 flex items-baseline justify-between">
											<h5 className="text-sm font-bold text-zinc-200">{comment.user.name}</h5>
											<span className="text-xs text-zinc-500">
												{new Date(comment.createdAt).toLocaleDateString()}
											</span>
										</div>
										<p className="text-sm leading-relaxed whitespace-pre-wrap text-zinc-300">
											{comment.content}
										</p>
									</div>
								</div>
							))
						)}
					</div>
				</div>
			)}
		</div>
	);
}
