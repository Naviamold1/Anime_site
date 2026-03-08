import { useState } from "react";
import { authClient } from "@/lib/auth-client";

interface User {
	id: string;
	name: string;
	email: string;
	image?: string | null;
}

export default function OnboardingForm({ user }: { user: User }) {
	const [name, setName] = useState(user.name);
	const [image, setImage] = useState(user.image || "");
	const [isSubmitting, setIsSubmitting] = useState(false);
	const [error, setError] = useState("");

	const handleSubmit = async (e: React.FormEvent) => {
		e.preventDefault();
		setIsSubmitting(true);
		setError("");

		try {
			// 1. Update basic info via Better Auth
			const { error: authError } = await authClient.updateUser({
				name,
				image,
			});

			if (authError) {
				throw new Error(authError.message);
			}

			// 2. Mark as onboarded via our custom API
			const onboardRes = await fetch("/api/user/onboard.json", {
				method: "POST",
			});

			if (!onboardRes.ok) {
				throw new Error("Failed to finalize onboarding. Please try again.");
			}

			// 3. Success! Redirect to home
			window.location.href = "/";
		} catch (err: unknown) {
			if (err instanceof Error) {
				setError(err.message);
			} else {
				setError("An unexpected error occurred.");
			}
		} finally {
			setIsSubmitting(false);
		}
	};

	return (
		<form onSubmit={handleSubmit} className="space-y-6">
			{error && (
				<div className="rounded-lg border border-red-500/20 bg-red-500/10 p-3 text-xs text-red-500">
					{error}
				</div>
			)}

			<div>
				<label
					htmlFor="username"
					className="mb-2 ml-1 block text-xs font-bold tracking-widest text-zinc-500 uppercase"
				>
					Display Name
				</label>
				<input
					id="username"
					type="text"
					value={name}
					onChange={(e) => setName(e.target.value)}
					placeholder="e.g. AnimeLover99"
					className="w-full rounded-xl border border-zinc-800 bg-zinc-950 px-4 py-3 text-white placeholder-zinc-600 transition-all focus:border-orange-500 focus:ring-2 focus:ring-orange-500/50 focus:outline-none"
					required
				/>
			</div>

			<div>
				<label
					htmlFor="avatar"
					className="mb-2 ml-1 block text-xs font-bold tracking-widest text-zinc-500 uppercase"
				>
					Avatar URL (Optional)
				</label>
				<input
					id="avatar"
					type="url"
					value={image}
					onChange={(e) => setImage(e.target.value)}
					placeholder="https://..."
					className="w-full rounded-xl border border-zinc-800 bg-zinc-950 px-4 py-3 text-white placeholder-zinc-600 transition-all focus:border-orange-500 focus:ring-2 focus:ring-orange-500/50 focus:outline-none"
				/>
				<p className="mt-2 px-1 text-[10px] leading-relaxed text-zinc-600">
					Provide a URL to an image you'd like to use as your profile picture.
				</p>
			</div>

			<div className="pt-2">
				<button
					type="submit"
					disabled={isSubmitting}
					className="w-full rounded-xl bg-orange-500 px-6 py-3 font-bold text-white shadow-lg shadow-orange-500/20 transition-all hover:bg-orange-600 active:scale-95 disabled:opacity-50"
				>
					{isSubmitting ? "Saving Profile..." : "Complete Setup"}
				</button>
			</div>
		</form>
	);
}
