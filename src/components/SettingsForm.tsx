import { useState } from "react";
import { authClient } from "@/lib/auth-client";

interface User {
	id: string;
	name: string;
	email: string;
	image?: string | null;
}

export default function SettingsForm({ user }: { user: User }) {
	const [name, setName] = useState(user.name);
	const [image, setImage] = useState(user.image || "");
	const [isSubmitting, setIsSubmitting] = useState(false);
	const [message, setMessage] = useState<{ type: "success" | "error"; text: string } | null>(null);

	const handleSubmit = async (e: React.FormEvent) => {
		e.preventDefault();
		setIsSubmitting(true);
		setMessage(null);

		try {
			const { error: authError } = await authClient.updateUser({
				name,
				image,
			});

			if (authError) {
				throw new Error(authError.message);
			}

			setMessage({ type: "success", text: "Profile updated successfully!" });
		} catch (err: unknown) {
			setMessage({
				type: "error",
				text: err instanceof Error ? err.message : "An unexpected error occurred.",
			});
		} finally {
			setIsSubmitting(false);
		}
	};

	return (
		<form onSubmit={handleSubmit} className="space-y-6">
			{message && (
				<div
					className={`rounded-lg border p-3 text-xs ${
						message.type === "success"
							? "border-green-500/20 bg-green-500/10 text-green-500"
							: "border-red-500/20 bg-red-500/10 text-red-500"
					}`}
				>
					{message.text}
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
					className="w-full rounded-xl border border-zinc-800 bg-zinc-950 px-4 py-3 text-white transition-all focus:border-orange-500 focus:ring-2 focus:ring-orange-500/50 focus:outline-none"
					required
				/>
			</div>

			<div>
				<label
					htmlFor="avatar"
					className="mb-2 ml-1 block text-xs font-bold tracking-widest text-zinc-500 uppercase"
				>
					Avatar URL
				</label>
				<input
					id="avatar"
					type="url"
					value={image}
					onChange={(e) => setImage(e.target.value)}
					className="w-full rounded-xl border border-zinc-800 bg-zinc-950 px-4 py-3 text-white transition-all focus:border-orange-500 focus:ring-2 focus:ring-orange-500/50 focus:outline-none"
				/>
			</div>

			<div className="pt-2">
				<button
					type="submit"
					disabled={isSubmitting}
					className="w-full rounded-xl bg-orange-500 px-6 py-3 font-bold text-white transition-all hover:bg-orange-600 active:scale-95 disabled:opacity-50"
				>
					{isSubmitting ? "Saving..." : "Save Changes"}
				</button>
			</div>
		</form>
	);
}
